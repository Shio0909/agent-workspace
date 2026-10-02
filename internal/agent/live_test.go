package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLiveCompactionKeepsEarlyFacts runs the agent against a real
// OpenAI-compatible endpoint with a window small enough that the session is
// compacted after a few turns. It is skipped unless the endpoint is given:
//
//	AGENT_LIVE_BASE_URL=https://host/v1 AGENT_LIVE_API_KEY=... AGENT_LIVE_MODEL=name \
//	  go test ./internal/agent -run Live -v -count=1
func TestLiveCompactionKeepsEarlyFacts(t *testing.T) {
	base, key, model := os.Getenv("AGENT_LIVE_BASE_URL"), os.Getenv("AGENT_LIVE_API_KEY"), os.Getenv("AGENT_LIVE_MODEL")
	if base == "" || key == "" {
		t.Skip("set AGENT_LIVE_BASE_URL and AGENT_LIVE_API_KEY to run against a real model")
	}
	creds := t.TempDir()
	if err := os.WriteFile(filepath.Join(creds, CredentialKey), []byte(key), 0o600); err != nil {
		t.Fatal(err)
	}
	a := New(Config{
		WorkspaceDir: t.TempDir(), CredentialDir: creds, LLMBaseURL: base, Model: model,
		LLMRetries: 2, ContextTokens: 1500, ReserveTokens: 300, MaxSteps: 4,
	})

	ctx := t.Context()
	say := func(text string) string {
		t.Helper()
		start := time.Now()
		reply, _, _, err := a.run(ctx, "s", text)
		if err != nil {
			t.Fatalf("%q: %v", text[:min(len(text), 40)], err)
		}
		t.Logf("%5.1fs  %-48.48q -> %.80q", time.Since(start).Seconds(), text, reply)
		return reply
	}

	say("Remember this for later: the staging cluster is called orca-staging, it listens on port 8443, " +
		"the on-call engineer this month is Priya, and we never deploy on Fridays. Reply with just OK.")
	for i := 0; i < 7; i++ {
		say(strings.Repeat("Routine status line with no decision in it. ", 14) + "Reply with just OK.")
	}

	stored, err := os.ReadFile(a.summaryPath("s"))
	if err != nil {
		t.Fatalf("no summary was stored after 8 turns in a 1500-token window: %v", err)
	}
	t.Logf("stored summary (%d bytes): %.200s", len(stored), stored)
	if strings.Contains(string(stored), "Raw conversation archive") {
		t.Errorf("the real summariser failed and the session fell back to the raw archive")
	}

	answer := say("What is the staging cluster called, which port does it listen on, who is on call, " +
		"and on which weekday do we not deploy? Answer in one line.")
	for _, want := range []string{"orca-staging", "8443", "Priya", "Friday"} {
		if !strings.Contains(answer, want) {
			t.Errorf("the answer lost %q after compaction: %q", want, answer)
		}
	}
}
