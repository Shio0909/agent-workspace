// Package fakellm is a deterministic OpenAI-compatible endpoint for tests and
// the kind demo. It accepts only the API keys it has been told about, so a key
// rotation can be exercised end to end: revoke the old key, and a workspace
// that did not pick up the new one gets a 401.
package fakellm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	mu       sync.Mutex
	keys     map[string]bool
	rejected int
	served   int

	// Summarisation requests (a system message that says "summarization
	// assistant") are counted apart from chat requests, so a test can tell the
	// size of the history the agent sends from the cost of shrinking it.
	summaries     int
	maxMessages   int
	failSummaries bool
	cutSummaries  bool

	// Every answer carries a usage object, so tests can assert exact token
	// counts; omitUsage makes it behave like a provider that sends none.
	omitUsage        bool
	promptTokens     int64
	completionTokens int64
}

// OmitUsage makes responses carry no usage object.
func (s *Server) OmitUsage(on bool) { s.mu.Lock(); s.omitUsage = on; s.mu.Unlock() }

// Tokens reports the usage the server has put into its answers so far. An
// agent which counts every response must end up with exactly these totals.
func (s *Server) Tokens() (prompt, completion int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.promptTokens, s.completionTokens
}

// tokens stands in for a tokenizer: one token per four bytes, rounded up.
func tokens(text string) int64 { return int64((len(text) + 3) / 4) }

// promptCost is the cost of the messages sent: four tokens of framing per
// message plus its content and tool calls.
func promptCost(msgs []chatMessage) int64 {
	var n int64
	for _, m := range msgs {
		n += 4 + tokens(m.Content) + tokens(string(m.ToolCalls))
	}
	return n
}

// usage books the cost of one answer and returns the OpenAI-style usage
// object, or nil if the server is set to omit it.
func (s *Server) usage(msgs []chatMessage, reply map[string]any) map[string]int64 {
	prompt := promptCost(msgs)
	completion := tokens(fmt.Sprint(reply["content"]))
	if calls, ok := reply["tool_calls"]; ok {
		b, _ := json.Marshal(calls)
		completion += tokens(string(b))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promptTokens += prompt
	s.completionTokens += completion
	if s.omitUsage {
		return nil
	}
	return map[string]int64{"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": prompt + completion}
}

// FailSummaries makes summarisation requests fail with a 500.
func (s *Server) FailSummaries(on bool) { s.mu.Lock(); s.failSummaries = on; s.mu.Unlock() }

// TruncateSummaries makes summarisation requests stop at the token cap.
func (s *Server) TruncateSummaries(on bool) { s.mu.Lock(); s.cutSummaries = on; s.mu.Unlock() }

// Stats reports how many summarisation requests were served and the longest
// message list any chat request carried.
func (s *Server) Stats() (summaries, maxMessages int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.summaries, s.maxMessages
}

func New(keys ...string) *Server {
	s := &Server{keys: map[string]bool{}}
	s.setKeys(keys)
	return s
}

func (s *Server) setKeys(keys []string) {
	s.keys = map[string]bool{}
	for _, k := range keys {
		if k = strings.TrimSpace(k); k != "" {
			s.keys[k] = true
		}
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.complete)
	mux.HandleFunc("PUT /admin/keys", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Keys []string `json:"keys"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.setKeys(in.Keys)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /admin/stats", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]int{
			"served": s.served, "rejected": s.rejected, "keys": len(s.keys),
			"summaries": s.summaries, "max_messages": s.maxMessages,
			"prompt_tokens": int(s.promptTokens), "completion_tokens": int(s.completionTokens),
		})
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprintln(w, "ok") })
	return mux
}

type chatMessage struct {
	Role       string          `json:"role"`
	Content    string          `json:"content"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}

// complete answers deterministically:
//   - "write <path>: <text>" asks for a write_file tool call;
//   - after the tool result it answers "saved";
//   - "sleep <ms> <text>" waits that long (at most 60s) before answering like
//     "<text>", which gives tests a turn that is reliably still in flight;
//   - "recall <text>" answers "recall(found): <text>" if <text> occurs in any
//     earlier message of the history it was sent, else "recall(missing): <text>",
//     which shows that earlier content really reached the model;
//   - anything else echoes the text with the number of user turns seen, which
//     shows that the agent replayed its persisted conversation.
//
// Every answer carries a usage object (see tokens and promptCost).
func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	s.mu.Lock()
	ok := s.keys[token]
	if ok {
		s.served++
	} else {
		s.rejected++
	}
	s.mu.Unlock()
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprint(w, `{"error":{"message":"invalid api key"}}`)
		return
	}
	var in struct {
		Messages []chatMessage `json:"messages"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&in); err != nil || len(in.Messages) == 0 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if in.Messages[0].Role == "system" && strings.Contains(in.Messages[0].Content, "summarization assistant") {
		s.summarize(w, in.Messages)
		return
	}
	s.mu.Lock()
	s.maxMessages = max(s.maxMessages, len(in.Messages))
	s.mu.Unlock()
	last := in.Messages[len(in.Messages)-1]
	if last.Role == "user" {
		last.Content = s.pause(r, last.Content)
	}
	reply := map[string]any{"role": "assistant"}
	switch {
	case last.Role == "tool":
		reply["content"] = "saved"
	case last.Role == "user" && strings.HasPrefix(last.Content, "write "):
		path, content, _ := strings.Cut(strings.TrimPrefix(last.Content, "write "), ":")
		args, _ := json.Marshal(map[string]string{"path": strings.TrimSpace(path), "content": strings.TrimSpace(content)})
		reply["content"] = ""
		reply["tool_calls"] = []map[string]any{{
			"id": "call_1", "type": "function",
			"function": map[string]any{"name": "write_file", "arguments": string(args)},
		}}
	case last.Role == "user" && strings.HasPrefix(last.Content, "recall "):
		needle := strings.TrimPrefix(last.Content, "recall ")
		found := false
		for _, m := range in.Messages[:len(in.Messages)-1] {
			found = found || strings.Contains(m.Content, needle)
		}
		reply["content"] = fmt.Sprintf("recall(%s): %s", map[bool]string{true: "found", false: "missing"}[found], needle)
	default:
		users := 0
		for _, m := range in.Messages {
			if m.Role == "user" {
				users++
			}
		}
		reply["content"] = fmt.Sprintf("echo(%d): %s", users, last.Content)
	}
	w.Header().Set("Content-Type", "application/json")
	body := map[string]any{"choices": []map[string]any{{"message": reply}}}
	if u := s.usage(in.Messages, reply); u != nil {
		body["usage"] = u
	}
	_ = json.NewEncoder(w).Encode(body)
}

// summarize answers a summarisation request with a summary that names the
// first words of every user line in the transcript, and says whether it was
// merged into a previous summary, so a test can check what was carried over.
func (s *Server) summarize(w http.ResponseWriter, msgs []chatMessage) {
	prompt := msgs[len(msgs)-1].Content
	s.mu.Lock()
	s.summaries++
	n, fail, cut := s.summaries, s.failSummaries, s.cutSummaries
	s.mu.Unlock()
	if fail {
		http.Error(w, "summarizer down", http.StatusInternalServerError)
		return
	}
	var users []string
	for _, line := range strings.Split(prompt, "\n") {
		if text, ok := strings.CutPrefix(line, "[User]: "); ok {
			text = strings.Join(strings.Fields(text), " ")
			users = append(users, text[:min(len(text), 12)])
		}
	}
	merged := strings.Contains(prompt, "<previous-summary>")
	choice := map[string]any{"message": map[string]any{
		"role": "assistant", "content": fmt.Sprintf("## Goal\nsummary#%d merged=%v users=[%s]", n, merged, strings.Join(users, "|")),
	}}
	if cut {
		choice["finish_reason"] = "length"
	}
	w.Header().Set("Content-Type", "application/json")
	body := map[string]any{"choices": []map[string]any{choice}}
	if u := s.usage(msgs, choice["message"].(map[string]any)); u != nil {
		body["usage"] = u
	}
	_ = json.NewEncoder(w).Encode(body)
}

// pause implements the "sleep <ms> <text>" prefix and returns the text to
// answer. It gives up early if the client goes away.
func (s *Server) pause(r *http.Request, content string) string {
	rest, ok := strings.CutPrefix(content, "sleep ")
	if !ok {
		return content
	}
	num, text, _ := strings.Cut(rest, " ")
	ms, err := strconv.Atoi(num)
	if err != nil || ms < 0 {
		return content
	}
	timer := time.NewTimer(time.Duration(min(ms, 60000)) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.Context().Done():
	}
	return text
}
