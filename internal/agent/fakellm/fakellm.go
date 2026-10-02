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
//   - anything else echoes the text with the number of user turns seen, which
//     shows that the agent replayed its persisted conversation.
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
		s.summarize(w, in.Messages[len(in.Messages)-1].Content)
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
	_ = json.NewEncoder(w).Encode(map[string]any{"choices": []map[string]any{{"message": reply}}})
}

// summarize answers a summarisation request with a summary that names the
// first words of every user line in the transcript, and says whether it was
// merged into a previous summary, so a test can check what was carried over.
func (s *Server) summarize(w http.ResponseWriter, prompt string) {
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
	_ = json.NewEncoder(w).Encode(map[string]any{"choices": []map[string]any{choice}})
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
