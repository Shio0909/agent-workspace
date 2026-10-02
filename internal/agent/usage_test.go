package agent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func (e *env) heartbeatUsage() Usage {
	e.t.Helper()
	rec := httptest.NewRecorder()
	e.handle.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/heartbeat", nil))
	var out struct {
		Usage Usage `json:"usage"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil {
		e.t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body)
	}
	return out.Usage
}

func TestUsageIsCountedExactlyAndReportedOnTheHeartbeat(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	if got := e.heartbeatUsage(); got != (Usage{}) {
		t.Fatalf("a new workspace has used nothing: %+v", got)
	}
	e.chat("s", "hi")
	// Hand-computed from the fake's rule (four tokens of framing per message,
	// one token per four bytes): the request is [user "hi"] = 5 prompt tokens
	// and the reply "echo(1): hi" is 3.
	if got, want := e.heartbeatUsage(), (Usage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8}); got != want {
		t.Fatalf("after one turn: got %+v want %+v", got, want)
	}
	e.chat("s", "again")
	// The second request replays the first turn: 5 + 7 + 6 prompt tokens, and
	// the reply "echo(2): again" is 4.
	if got, want := e.heartbeatUsage(), (Usage{PromptTokens: 23, CompletionTokens: 7, TotalTokens: 30}); got != want {
		t.Fatalf("after two turns: got %+v want %+v", got, want)
	}
	if prompt, completion := e.llm.Tokens(); prompt != 23 || completion != 7 {
		t.Fatalf("the provider billed %d/%d, the agent counted something else", prompt, completion)
	}
}

func TestUsageSurvivesARestart(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	e.chat("s", "hi")
	before := e.heartbeatUsage()

	e.restart(e.agentLLM())
	if got := e.heartbeatUsage(); got != before {
		t.Fatalf("the counter was reset by a new pod: %+v -> %+v", before, got)
	}
	e.chat("s", "again")
	if got, want := e.heartbeatUsage(), (Usage{PromptTokens: 23, CompletionTokens: 7, TotalTokens: 30}); got != want {
		t.Fatalf("the new pod did not continue the old total: got %+v want %+v", got, want)
	}
	// The file is where a second pod finds it, and the model's file tools
	// cannot reach it.
	if _, err := os.Stat(filepath.Join(e.work, "usage.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.agent.dispatch("write_file", `{"path":"../usage.json","content":"{}"}`); err == nil {
		t.Fatal("a tool escaped files/ and could rewrite the counter")
	}
	if _, err := e.agent.dispatch("write_file", `{"path":"usage.json","content":"{}"}`); err != nil {
		t.Fatal(err)
	}
	e.restart(e.agentLLM())
	if got := e.heartbeatUsage(); got.TotalTokens != 30 {
		t.Fatalf("a file written by a tool replaced the counter: %+v", got)
	}
}

func TestSummarisationCallsAreMetered(t *testing.T) {
	e := compactingEnv(t)
	e.turns(0, 10)
	if summaries, _ := e.llm.Stats(); summaries == 0 {
		t.Fatal("the scenario never compacted, so it proves nothing")
	}
	prompt, completion := e.llm.Tokens()
	got := e.heartbeatUsage()
	if got.PromptTokens != prompt || got.CompletionTokens != completion || got.TotalTokens != prompt+completion {
		t.Fatalf("summarisation cost is missing from the count: agent %+v, provider %d/%d", got, prompt, completion)
	}
}

func TestResponsesWithoutUsageAreNotGuessed(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	e.llm.OmitUsage(true)
	if code, _ := e.chat("s", "hi"); code != http.StatusOK {
		t.Fatalf("a provider which sends no usage must still work: %d", code)
	}
	if got := e.heartbeatUsage(); got != (Usage{}) {
		t.Fatalf("usage was invented: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(e.work, "usage.json")); !os.IsNotExist(err) {
		t.Fatalf("nothing was counted, so nothing should be written: %v", err)
	}
}

func TestRefusedCallsCostNothing(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "revoked")
	if code, _ := e.chat("s", "hi"); code != http.StatusBadGateway {
		t.Fatalf("got %d", code)
	}
	if got := e.heartbeatUsage(); got != (Usage{}) {
		t.Fatalf("a 401 was counted: %+v", got)
	}
}

func TestUnreadableUsageFileRestartsTheCountWithoutFailingTheAgent(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	if err := os.WriteFile(filepath.Join(e.work, "usage.json"), []byte(`{"total_tokens":-5`), 0o600); err != nil {
		t.Fatal(err)
	}
	e.restart(e.agentLLM())
	if code, _ := e.chat("s", "hi"); code != http.StatusOK {
		t.Fatalf("got %d", code)
	}
	if got := e.heartbeatUsage(); got.TotalTokens != 8 {
		t.Fatalf("got %+v", got)
	}
}

func TestProviderUsageIsNormalisedBeforeItIsAdded(t *testing.T) {
	m := loadUsage("")
	m.add(Usage{PromptTokens: 10, CompletionTokens: 4}) // total omitted
	m.add(Usage{PromptTokens: -1, TotalTokens: 99})     // nonsense is dropped whole
	m.add(Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 9})
	if got, want := m.snapshot(), (Usage{PromptTokens: 11, CompletionTokens: 5, TotalTokens: 23}); got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}
