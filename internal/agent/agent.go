// Package agent is a small ReAct agent that runs inside a workspace. It exists
// so the controller manages a real agent workload: one that holds an LLM
// credential, keeps conversation state on the workspace volume, and has to
// survive a key rotation without restarting.
package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	// CredentialKey is the file name of the LLM API key inside CredentialDir.
	CredentialKey = "llm_api_key"
	// VersionKey is written by the controller next to the credentials.
	VersionKey = ".version"

	maxMessageBytes = 8 << 10
	maxFileBytes    = 64 << 10
	maxToolOutput   = 4 << 10
)

var sessionPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ErrNoCredential means the credential file is missing or empty.
var ErrNoCredential = errors.New("llm_api_key credential is not configured")

// ErrCredentialRejected means the LLM endpoint refused the key.
var ErrCredentialRejected = errors.New("llm endpoint rejected the credential")

type Config struct {
	WorkspaceID   string
	WorkspaceDir  string // persistent volume; sessions and files live here
	CredentialDir string // read-only Secret mount
	LLMBaseURL    string // OpenAI-compatible base URL, without /chat/completions
	Model         string
	MaxSteps      int
	HTTPClient    *http.Client
}

type Agent struct {
	cfg      Config
	sessions sync.Map // session id -> *sync.Mutex
}

func New(cfg Config) *Agent {
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 6
	}
	if cfg.Model == "" {
		cfg.Model = "default"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &Agent{cfg: cfg}
}

type message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []toolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type toolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// Credential is one consistent snapshot of the mounted credential files.
type Credential struct {
	Version int
	APIKey  string
}

// LoadCredential reads the key together with its version. The kubelet swaps
// the whole directory atomically, but two separate file reads can still
// straddle a swap, so the version is read on both sides of the key and the
// read is retried if it moved.
func (a *Agent) LoadCredential() (Credential, error) {
	for i := 0; i < 5; i++ {
		before := a.version()
		key, err := os.ReadFile(filepath.Join(a.cfg.CredentialDir, CredentialKey))
		if errors.Is(err, os.ErrNotExist) || (err == nil && strings.TrimSpace(string(key)) == "") {
			return Credential{Version: before}, ErrNoCredential
		}
		if err != nil {
			return Credential{Version: before}, err
		}
		if after := a.version(); after == before {
			return Credential{Version: before, APIKey: strings.TrimSpace(string(key))}, nil
		}
	}
	return Credential{}, errors.New("credential files kept changing while being read")
}

func (a *Agent) version() int {
	b, err := os.ReadFile(filepath.Join(a.cfg.CredentialDir, VersionKey))
	if err != nil {
		return 0
	}
	var v int
	_, _ = fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &v)
	return v
}

func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()
	// Health does not depend on the credential: a workspace must become ready
	// before its key is set, otherwise the key could never be delivered.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /status", a.status)
	mux.HandleFunc("POST /v1/chat", a.chat)
	return mux
}

func (a *Agent) status(w http.ResponseWriter, r *http.Request) {
	cred, err := a.LoadCredential()
	entries, _ := os.ReadDir(filepath.Join(a.cfg.WorkspaceDir, "sessions"))
	writeJSON(w, http.StatusOK, map[string]any{
		"workspace":          a.cfg.WorkspaceID,
		"credential_version": cred.Version,
		"has_credential":     err == nil,
		"sessions":           len(entries),
		"pid":                os.Getpid(),
	})
}

func (a *Agent) chat(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Session string `json:"session"`
		Message string `json:"message"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxMessageBytes+1024))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil || !sessionPattern.MatchString(in.Session) || in.Message == "" || len(in.Message) > maxMessageBytes {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "session must match [A-Za-z0-9_-]{1,64} and message must be 1 to 8192 bytes"})
		return
	}
	lock, _ := a.sessions.LoadOrStore(in.Session, &sync.Mutex{})
	lock.(*sync.Mutex).Lock()
	defer lock.(*sync.Mutex).Unlock()

	reply, steps, version, err := a.run(r.Context(), in.Session, in.Message)
	switch {
	case errors.Is(err, ErrNoCredential):
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error(), "credential_version": version})
	case errors.Is(err, ErrCredentialRejected):
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "credential_version": version})
	case err != nil:
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "credential_version": version})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"reply": reply, "steps": steps, "credential_version": version, "pid": os.Getpid()})
	}
}

// run executes one user turn: call the model, run any tools it asks for, and
// repeat until it answers or MaxSteps is reached. The credential is reloaded
// before every model call, so a rotation takes effect mid-conversation without
// a restart. It returns the version used by the last call.
func (a *Agent) run(ctx context.Context, session, text string) (string, int, int, error) {
	history, err := a.loadSession(session)
	if err != nil {
		return "", 0, 0, err
	}
	// A turn is written to the session only once it has a final answer. If
	// the model call fails (for example a revoked key) and the caller retries,
	// the retries must not pile up unanswered user messages in the history.
	var turn []message
	add := func(m message) {
		turn = append(turn, m)
		history = append(history, m)
	}
	add(message{Role: "user", Content: text})
	version := 0
	for step := 1; step <= a.cfg.MaxSteps; step++ {
		cred, err := a.LoadCredential()
		version = cred.Version
		if err != nil {
			return "", step - 1, version, err
		}
		reply, err := a.complete(ctx, cred.APIKey, history)
		if err != nil {
			return "", step - 1, version, err
		}
		add(reply)
		if len(reply.ToolCalls) == 0 {
			if err := a.appendSession(session, turn); err != nil {
				return "", step, version, err
			}
			return reply.Content, step, version, nil
		}
		for _, call := range reply.ToolCalls {
			add(message{Role: "tool", ToolCallID: call.ID, Content: a.runTool(call)})
		}
	}
	return "", a.cfg.MaxSteps, version, fmt.Errorf("no final answer after %d steps", a.cfg.MaxSteps)
}

func (a *Agent) complete(ctx context.Context, apiKey string, history []message) (message, error) {
	body, err := json.Marshal(map[string]any{"model": a.cfg.Model, "messages": history, "tools": toolSpecs()})
	if err != nil {
		return message{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(a.cfg.LLMBaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return message{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := a.cfg.HTTPClient.Do(req)
	if err != nil {
		return message{}, fmt.Errorf("call llm: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return message{}, ErrCredentialRejected
	}
	if resp.StatusCode != http.StatusOK {
		return message{}, fmt.Errorf("llm returned status %d", resp.StatusCode)
	}
	var out struct {
		Choices []struct {
			Message message `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil || len(out.Choices) == 0 {
		return message{}, errors.New("llm returned an unreadable response")
	}
	m := out.Choices[0].Message
	m.Role = "assistant"
	return m, nil
}

func (a *Agent) sessionPath(session string) string {
	return filepath.Join(a.cfg.WorkspaceDir, "sessions", session+".jsonl")
}

func (a *Agent) loadSession(session string) ([]message, error) {
	f, err := os.Open(a.sessionPath(session))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []message
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		var m message
		// A line torn by a crash is skipped instead of failing the session.
		if json.Unmarshal(sc.Bytes(), &m) == nil && m.Role != "" {
			out = append(out, m)
		}
	}
	// Keep only complete turns. A crash can cut a turn after a tool call but
	// before its result; replaying that to the model is an API error.
	for len(out) > 0 {
		if last := out[len(out)-1]; last.Role == "assistant" && len(last.ToolCalls) == 0 {
			break
		}
		out = out[:len(out)-1]
	}
	return out, sc.Err()
}

// appendSession writes a whole turn with one write and one fsync.
func (a *Agent) appendSession(session string, turn []message) error {
	if err := os.MkdirAll(filepath.Dir(a.sessionPath(session)), 0o750); err != nil {
		return err
	}
	var buf bytes.Buffer
	// If a crash left half a line, end it first: otherwise the first record of
	// this turn would be glued onto the fragment and lost with it.
	if b, err := os.ReadFile(a.sessionPath(session)); err == nil && len(b) > 0 && b[len(b)-1] != '\n' {
		buf.WriteByte('\n')
	}
	for _, m := range turn {
		b, err := json.Marshal(m)
		if err != nil {
			return err
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	f, err := os.OpenFile(a.sessionPath(session), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(buf.Bytes()); err != nil {
		return err
	}
	return f.Sync()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
