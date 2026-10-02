package agent

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// compactingEnv is an agent with a window small enough that a few turns of
// ordinary text fill it. The tool list alone is a fifth of it.
func compactingEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	e.mod = func(c *Config) { c.ContextTokens, c.ReserveTokens, c.KeepRecentTokens = 1000, 200, 200 }
	e.restart(e.agentLLM())
	return e
}

func turnText(i int) string {
	return fmt.Sprintf("turn%02d %s", i, strings.Repeat("alpha beta gamma ", 12))
}

func (e *env) turns(from, to int) {
	e.t.Helper()
	for i := from; i < to; i++ {
		if code, out := e.chat("s", turnText(i)); code != http.StatusOK {
			e.t.Fatalf("turn %d: %d %v", i, code, out)
		}
	}
}

func (e *env) storedSummary() (summaryFile, bool) {
	b, err := os.ReadFile(e.agent.summaryPath("s"))
	if err != nil {
		return summaryFile{}, false
	}
	var f summaryFile
	if err := json.Unmarshal(b, &f); err != nil {
		e.t.Fatal(err)
	}
	return f, true
}

func TestCompactionKeepsTheHistorySentToTheModelBounded(t *testing.T) {
	const n = 25

	// Without a window the whole session is replayed: n-1 earlier turns of two
	// messages each, plus the new one.
	plain := newEnv(t, "k1")
	plain.setCredential(1, "k1")
	plain.turns(0, n)
	if summaries, longest := plain.llm.Stats(); summaries != 0 || longest != 2*(n-1)+1 {
		t.Fatalf("control: summaries=%d longest=%d", summaries, longest)
	}

	e := compactingEnv(t)
	e.turns(0, n)
	summaries, longest := e.llm.Stats()
	t.Logf("%d turns: %d summarisation calls, longest history sent %d messages (uncompacted: %d)", n, summaries, longest, 2*(n-1)+1)
	if summaries < 2 {
		t.Fatalf("a session this long must be compacted more than once, got %d", summaries)
	}
	// Threshold 800 tokens, minus the tool list and the new message, holds a
	// handful of ~60-token messages; the exact figure is deterministic.
	if longest > 12 {
		t.Fatalf("the model was sent up to %d messages although the window holds about ten", longest)
	}
}

func TestSummaryIsStoredAndReusedAfterARestart(t *testing.T) {
	e := compactingEnv(t)
	e.turns(0, 8)
	stored, ok := e.storedSummary()
	if !ok || stored.Covers == 0 || stored.Degraded {
		t.Fatalf("expected a stored summary after 8 turns: %+v ok=%v", stored, ok)
	}
	before, _ := e.llm.Stats()

	e.restart(e.agentLLM())
	// The fake counts the user messages it is sent. After a restart the history
	// is the summary, the turns the summary does not cover, and the new message.
	records, _ := e.agent.loadSession("s")
	want := 1 + 1
	for _, m := range records[stored.Covers:] {
		if m.Role == "user" {
			want++
		}
	}
	if code, out := e.chat("s", "short question"); code != http.StatusOK || out["reply"] != fmt.Sprintf("echo(%d): short question", want) {
		t.Fatalf("want echo(%d): %d %v", want, code, out)
	}
	if after, _ := e.llm.Stats(); after != before {
		t.Fatalf("the stored summary was not reused: %d -> %d summarisation calls", before, after)
	}

	// The summary lives outside sessions/, which /status counts.
	rec := httptest.NewRecorder()
	e.handle.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	if !strings.Contains(rec.Body.String(), `"sessions":1`) {
		t.Fatalf("status counts the summary as a session: %s", rec.Body)
	}
}

func TestSecondCompactionUpdatesTheSummaryInsteadOfSummarisingIt(t *testing.T) {
	e := compactingEnv(t)
	e.turns(0, 25)
	stored, _ := e.storedSummary()
	// The fake reports whether the request carried a <previous-summary>, and
	// the first words of every user line in the transcript it was given.
	if !strings.Contains(stored.Summary, "merged=true") {
		t.Fatalf("a later compaction must update the earlier summary: %s", stored.Summary)
	}
	if strings.Contains(stored.Summary, "turn00") || strings.Contains(stored.Summary, "compacted into") {
		t.Fatalf("the previous summary was fed back in as conversation: %s", stored.Summary)
	}
}

func TestSummarizerFailureFallsBackToARawArchive(t *testing.T) {
	e := compactingEnv(t)
	e.llm.FailSummaries(true)
	e.turns(0, 10) // every chat still succeeds
	stored, ok := e.storedSummary()
	if !ok || !stored.Degraded || !strings.Contains(stored.Summary, "Raw conversation archive") {
		t.Fatalf("expected a degraded archive: %+v ok=%v", stored, ok)
	}
	if got, budget := estimateTokens(stored.Summary), e.agent.cfg.summaryBudget(); got > budget {
		t.Fatalf("the archive is %d tokens, over the %d budget, so it would not shrink the context", got, budget)
	}
	if _, longest := e.llm.Stats(); longest > 12 {
		t.Fatalf("a failing summariser must not let the history grow: %d messages", longest)
	}
}

func TestSummaryCutOffByTheTokenCapIsNotTrusted(t *testing.T) {
	e := compactingEnv(t)
	e.llm.TruncateSummaries(true)
	e.turns(0, 8)
	stored, _ := e.storedSummary()
	if !stored.Degraded || strings.Contains(stored.Summary, "summary#") {
		t.Fatalf("a truncated summary became the checkpoint: %+v", stored)
	}
}

func TestNothingToCompactMakesNoModelCall(t *testing.T) {
	e := newEnv(t, "k1")
	e.setCredential(1, "k1")
	// The tool list alone is over this threshold, and there is no earlier turn
	// to summarise: the only option is to send the turn as it is.
	e.mod = func(c *Config) { c.ContextTokens, c.ReserveTokens = 300, 50 }
	e.restart(e.agentLLM())
	for i := 0; i < 2; i++ {
		if code, out := e.chat("s", turnText(i)); code != http.StatusOK {
			t.Fatalf("turn %d: %d %v", i, code, out)
		}
	}
	if summaries, _ := e.llm.Stats(); summaries != 0 {
		t.Fatalf("summarised %d times with nothing outside the keep-recent budget", summaries)
	}
}

func TestRejectedCredentialDuringCompactionIsReportedNotDegraded(t *testing.T) {
	e := compactingEnv(t)
	e.turns(0, 6)
	before, _ := e.storedSummary()
	e.handleAdmin(httptest.NewRecorder(), `{"keys":["other"]}`)
	code, out := e.chat("s", turnText(6))
	if code != http.StatusBadGateway || out["credential_version"].(float64) != 1 {
		t.Fatalf("a refused key must surface as such: %d %v", code, out)
	}
	if after, _ := e.storedSummary(); after != before {
		t.Fatalf("a refused key must not rewrite the summary: %+v -> %+v", before, after)
	}
}

func TestCutPointFallsOnATurnStartAndKeepsToolPairsTogether(t *testing.T) {
	user := func(s string) message { return message{Role: "user", Content: s} }
	asst := func(s string) message { return message{Role: "assistant", Content: s} }
	callMsg := message{Role: "assistant", ToolCalls: []toolCall{{ID: "c"}}}
	tool := message{Role: "tool", ToolCallID: "c", Content: strings.Repeat("x", 400)}
	recs := []message{
		user("a"), asst("b"), // 0,1
		user("c"), callMsg, tool, asst("d"), // 2..5
		user("e"), asst("f"), // 6,7
	}
	cases := []struct {
		keep int
		want int
	}{
		{keep: 1 << 20, want: 0}, // everything fits: nothing to summarise
		{keep: 0, want: 6},       // nothing fits: the newest turn is kept anyway
		{keep: 20, want: 6},      // room for the last turn only
		{keep: 150, want: 6},     // the middle turn's tool result does not fit
	}
	for _, tc := range cases {
		got := cutPoint(recs, 0, tc.keep)
		if got != tc.want || recs[got].Role != "user" {
			t.Errorf("keep=%d: cut=%d, want %d (a user message)", tc.keep, got, tc.want)
		}
	}
	if got := cutPoint(recs, 2, 1<<20); got != 2 {
		t.Errorf("start must bound the search: %d", got)
	}
	if got := cutPoint(nil, 0, 100); got != 0 {
		t.Errorf("empty log: %d", got)
	}
}

func TestStoredSummaryThatDoesNotMatchTheLogIsIgnored(t *testing.T) {
	e := compactingEnv(t)
	e.turns(0, 3)
	records, _ := e.agent.loadSession("s")
	for name, f := range map[string]summaryFile{
		"covers past the end": {Summary: "S", Covers: len(records) + 3},
		"covers everything":   {Summary: "S", Covers: len(records)},
		"covers nothing":      {Summary: "S", Covers: 0},
		"cut inside a turn":   {Summary: "S", Covers: 1},
		"empty summary":       {Summary: " ", Covers: 2},
	} {
		if err := e.agent.saveSummary("s", sessionState{summary: f.Summary, covers: f.Covers}); err != nil {
			t.Fatal(err)
		}
		if st := e.agent.loadState("s", records); st.summary != "" || st.covers != 0 {
			t.Errorf("%s: the summary was applied and would leave a hole in the history: %+v", name, st)
		}
	}
}

func TestCompactionDefaultsKeepTheBudgetsConsistent(t *testing.T) {
	c := Config{ContextTokens: 8000}.withCompactionDefaults()
	if c.ReserveTokens != defaultReserveTokens || c.KeepRecentTokens != (8000-defaultReserveTokens)/4 {
		t.Fatalf("%+v", c)
	}
	if c.summaryBudget() > c.KeepRecentTokens {
		t.Fatalf("the summary must not outgrow the retained tail: %d > %d", c.summaryBudget(), c.KeepRecentTokens)
	}
	// A keep-recent budget above a quarter of the usable window would leave the
	// compacted context above the threshold again.
	if c := (Config{ContextTokens: 4000, ReserveTokens: 1000, KeepRecentTokens: 9000}).withCompactionDefaults(); c.KeepRecentTokens != 750 {
		t.Fatalf("keep-recent not capped: %+v", c)
	}
	if c := (Config{ContextTokens: 1000, ReserveTokens: 5000}).withCompactionDefaults(); c.ReserveTokens != 500 {
		t.Fatalf("a reserve larger than the window was not clamped: %+v", c)
	}
	if off := (Config{}).withCompactionDefaults(); off.KeepRecentTokens != 0 || off.ReserveTokens != 0 {
		t.Fatalf("compaction must stay off without a window: %+v", off)
	}
}
