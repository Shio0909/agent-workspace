package agent

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-workspace/internal/agent/fakellm"
)

type env struct {
	t      *testing.T
	llm    *fakellm.Server
	agent  *Agent
	handle http.Handler
	work   string
	creds  string
	mod    func(*Config) // applied on every restart, for tests that need a non-default Config
}

func newEnv(t *testing.T, keys ...string) *env {
	t.Helper()
	llm := fakellm.New(keys...)
	srv := httptest.NewServer(llm.Handler())
	t.Cleanup(srv.Close)
	e := &env{t: t, llm: llm, work: t.TempDir(), creds: t.TempDir()}
	e.restart(srv.URL + "/v1")
	return e
}

// restart builds a fresh Agent on the same directories, which is what a pod
// restart looks like: only the volumes survive.
func (e *env) restart(llmURL string) {
	cfg := Config{WorkspaceID: "a1", WorkspaceDir: e.work, CredentialDir: e.creds, LLMBaseURL: llmURL, Model: "m"}
	if e.mod != nil {
		e.mod(&cfg)
	}
	e.agent = New(cfg)
	e.handle = e.agent.Handler()
}

func (e *env) setCredential(version int, key string) {
	e.t.Helper()
	if err := os.WriteFile(filepath.Join(e.creds, CredentialKey), []byte(key), 0o600); err != nil {
		e.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(e.creds, VersionKey), []byte(itoa(version)), 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func (e *env) chat(session, text string) (int, map[string]any) {
	e.t.Helper()
	body, _ := json.Marshal(map[string]string{"session": session, "message": text})
	rec := httptest.NewRecorder()
	e.handle.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat", bytes.NewReader(body)))
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestReadyWithoutCredentialButChatNeedsOne(t *testing.T) {
	e := newEnv(t, "k1")
	rec := httptest.NewRecorder()
	e.handle.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatal("health must not depend on the credential")
	}
	code, out := e.chat("s", "hello")
	if code != http.StatusServiceUnavailable || !strings.Contains(out["error"].(string), "not configured") {
		t.Fatalf("code=%d out=%v", code, out)
	}
}

func TestRotationTakesEffectWithoutRestart(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1\n") // trailing newline, as a file written by hand would have
	code, out := e.chat("s", "hello")
	if code != http.StatusOK || out["credential_version"].(float64) != 1 {
		t.Fatalf("first chat: %d %v", code, out)
	}

	// The provider revokes k1. The agent still holds version 1, so it is
	// refused, and the failure names the version it used.
	e.handleAdmin(httptest.NewRecorder(), `{"keys":["k2"]}`)
	code, out = e.chat("s", "again")
	if code != http.StatusBadGateway || out["credential_version"].(float64) != 1 {
		t.Fatalf("revoked key should be refused: %d %v", code, out)
	}

	// The controller delivers version 2. The same Agent value, no restart.
	e.setCredential(2, "k2")
	code, out = e.chat("s", "third")
	if code != http.StatusOK || out["credential_version"].(float64) != 2 {
		t.Fatalf("after rotation: %d %v", code, out)
	}
}

func (e *env) handleAdmin(rec *httptest.ResponseRecorder, body string) {
	req := httptest.NewRequest(http.MethodPut, "/admin/keys", strings.NewReader(body))
	e.llm.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		e.t.Fatalf("admin: %d", rec.Code)
	}
}

func TestConversationSurvivesRestart(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	if code, out := e.chat("s", "one"); code != 200 || out["reply"] != "echo(1): one" {
		t.Fatalf("%d %v", code, out)
	}
	e.restart(e.agentLLM())
	// The fake counts user turns in the history it is sent: 2 means the new
	// process replayed the first turn from the volume.
	if code, out := e.chat("s", "two"); code != 200 || out["reply"] != "echo(2): two" {
		t.Fatalf("history was lost: %d %v", code, out)
	}
	if code, out := e.chat("other", "x"); code != 200 || out["reply"] != "echo(1): x" {
		t.Fatalf("sessions must be independent: %d %v", code, out)
	}
}

func (e *env) agentLLM() string { return e.agent.cfg.LLMBaseURL }

func TestToolCallWritesConfinedFileAndTornHistoryLineIsSkipped(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	code, out := e.chat("s", "write notes/a.txt: hello")
	if code != 200 || out["reply"] != "saved" || out["steps"].(float64) != 2 {
		t.Fatalf("%d %v", code, out)
	}
	b, err := os.ReadFile(filepath.Join(e.work, "files", "notes", "a.txt"))
	if err != nil || string(b) != "hello" {
		t.Fatalf("file=%q err=%v", b, err)
	}

	// A crash can leave half a line behind. The next turn must still work.
	f, _ := os.OpenFile(e.agent.sessionPath("s"), os.O_APPEND|os.O_WRONLY, 0o640)
	_, _ = f.WriteString(`{"role":"user","con`)
	_ = f.Close()
	// "echo(2)" proves the new turn was not glued onto the fragment and that
	// the earlier completed turn is still in the history.
	if code, out := e.chat("s", "after crash"); code != 200 || out["reply"] != "echo(2): after crash" {
		t.Fatalf("torn line broke the session: %d %v", code, out)
	}
}

func TestFailedTurnsAreNotPersisted(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	e.handleAdmin(httptest.NewRecorder(), `{"keys":["other"]}`) // k1 is now refused
	for i := 0; i < 5; i++ {
		if code, _ := e.chat("s", "retry"); code != http.StatusBadGateway {
			t.Fatalf("attempt %d: %d", i, code)
		}
	}
	e.handleAdmin(httptest.NewRecorder(), `{"keys":["k1"]}`)
	// Five failed retries must not show up as five user turns.
	if code, out := e.chat("s", "finally"); code != 200 || out["reply"] != "echo(1): finally" {
		t.Fatalf("failed turns polluted the history: %d %v", code, out)
	}
}

func TestIncompleteTurnIsDroppedOnReplay(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	if code, _ := e.chat("s", "first"); code != 200 {
		t.Fatal("setup")
	}
	// A crash after the model asked for a tool but before the result.
	cut := `{"role":"user","content":"write x.txt: y"}` + "\n" +
		`{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"write_file","arguments":"{}"}}]}` + "\n"
	f, _ := os.OpenFile(e.agent.sessionPath("s"), os.O_APPEND|os.O_WRONLY, 0o640)
	_, _ = f.WriteString(cut)
	_ = f.Close()
	if code, out := e.chat("s", "second"); code != 200 || out["reply"] != "echo(2): second" {
		t.Fatalf("incomplete turn was replayed: %d %v", code, out)
	}
}

func TestToolsCannotEscapeTheFilesDirectory(t *testing.T) {
	e := newEnv(t, "k1")
	if err := os.MkdirAll(filepath.Join(e.work, "sessions"), 0o750); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(e.work, "sessions", "private.jsonl")
	if err := os.WriteFile(secret, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"../sessions/private.jsonl", "/etc/passwd", "a/../../sessions/private.jsonl", "..", ""} {
		if out := e.agent.runTool(call("read_file", `{"path":`+quote(p)+`}`)); !strings.HasPrefix(out, "error:") {
			t.Errorf("read_file %q: %q", p, out)
		}
		if out := e.agent.runTool(call("write_file", `{"path":`+quote(p)+`,"content":"x"}`)); !strings.HasPrefix(out, "error:") {
			t.Errorf("write_file %q: %q", p, out)
		}
	}
	// A symlink planted inside files/ that points out must not be followed.
	if err := os.MkdirAll(filepath.Join(e.work, "files"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(e.work, "files", "link")); err != nil {
		t.Fatal(err)
	}
	if out := e.agent.runTool(call("read_file", `{"path":"link"}`)); strings.Contains(out, "private") {
		t.Fatalf("symlink escaped: %q", out)
	}
	if got, _ := os.ReadFile(secret); string(got) != "private" {
		t.Fatal("a write escaped the files directory")
	}
}

func call(name, args string) toolCall {
	var c toolCall
	c.Function.Name, c.Function.Arguments = name, args
	return c
}

func quote(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestStatusReportsVersionNotValue(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(7, "sk-do-not-print")
	rec := httptest.NewRecorder()
	e.handle.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	if !strings.Contains(rec.Body.String(), `"credential_version":7`) || strings.Contains(rec.Body.String(), "sk-do-not-print") {
		t.Fatalf("status=%s", rec.Body)
	}
}

func TestChatInputValidation(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	for _, tc := range []struct{ session, msg string }{{"", "x"}, {"../x", "x"}, {"ok", ""}, {strings.Repeat("a", 65), "x"}, {"ok", strings.Repeat("m", maxMessageBytes+1)}} {
		if code, _ := e.chat(tc.session, tc.msg); code != http.StatusBadRequest {
			t.Errorf("session=%q len(msg)=%d: %d", tc.session, len(tc.msg), code)
		}
	}
}

func TestHeartbeatReportsBusyOnlyWhileAChatIsInFlight(t *testing.T) {
	e := newEnv(t, "k")
	e.agent.cfg.Version = "agent-v7"
	e.handle = e.agent.Handler()
	beat := func() (bool, string) {
		rec := httptest.NewRecorder()
		e.handle.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/heartbeat", nil))
		var out struct {
			Busy    bool   `json:"busy"`
			Version string `json:"version"`
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
			t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body)
		}
		return out.Busy, out.Version
	}
	if busy, version := beat(); busy || version != "agent-v7" {
		t.Fatalf("idle agent: busy=%v version=%q", busy, version)
	}
	e.agent.inflight.Add(1) // a chat is being worked on
	if busy, _ := beat(); !busy {
		t.Fatal("an agent with a chat in flight must report busy")
	}
	e.agent.inflight.Add(-1)
	if busy, _ := beat(); busy {
		t.Fatal("busy must clear when the chat ends")
	}
}

func TestInflightCounterIsReleasedAfterEveryChatOutcome(t *testing.T) {
	e := newEnv(t, "k")
	e.setCredential(1, "k")
	e.chat("s", "hello")                  // success
	e.chat("bad session!", "hello")       // rejected before the counter
	e.setCredential(2, "revoked-by-peer") // provider will answer 401
	e.chat("s", "hello again")
	if n := e.agent.inflight.Load(); n != 0 {
		t.Fatalf("inflight leaked: %d", n)
	}
}
