// Package fakellm is a deterministic OpenAI-compatible endpoint for tests and
// the kind demo. It accepts only the API keys it has been told about, so a key
// rotation can be exercised end to end: revoke the old key, and a workspace
// that did not pick up the new one gets a 401.
package fakellm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

type Server struct {
	mu       sync.Mutex
	keys     map[string]bool
	rejected int
	served   int
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
		_ = json.NewEncoder(w).Encode(map[string]int{"served": s.served, "rejected": s.rejected, "keys": len(s.keys)})
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
	last := in.Messages[len(in.Messages)-1]
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
