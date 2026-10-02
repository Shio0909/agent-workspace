package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func usageJSON(prompt, completion, total int64) string {
	b, _ := json.Marshal(Usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: total})
	return string(b)
}

func (e *beatEnv) report(usage string) {
	e.t.Helper()
	e.w.usage.Store(usage)
	e.beat()
}

func (e *beatEnv) setBudget(n int64) Workspace {
	e.t.Helper()
	w, err := e.c.SetTokenBudget(testActor, "a1", n)
	if err != nil {
		e.t.Fatal(err)
	}
	return w
}

func (e *beatEnv) count(action string) int {
	e.t.Helper()
	events, err := e.c.Audit(AuditQuery{Workspace: "a1", Action: action})
	if err != nil {
		e.t.Fatal(err)
	}
	return len(events)
}

// persisted reads what the store holds, without the unflushed overlay that Get adds.
func (e *beatEnv) persisted() Workspace {
	e.t.Helper()
	w, err := e.c.store.Get("a1")
	if err != nil {
		e.t.Fatal(err)
	}
	return w
}

func (e *beatEnv) reconcile() Workspace {
	e.t.Helper()
	if _, err := e.c.Reconcile(context.Background(), "a1"); err != nil {
		e.t.Fatal(err)
	}
	return e.get()
}

// restartController builds a second controller on the same data directory and
// the same runtime, which is what a restarted controller process looks like.
func (e *beatEnv) restartController() {
	e.t.Helper()
	if err := e.c.store.Close(); err != nil {
		e.t.Fatal(err)
	}
	s, err := OpenStore(e.dir)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = s.Close() })
	c := New(s, e.r, map[string]Profile{"agent": {HeartbeatPath: "/heartbeat"}}, 24*time.Hour)
	c.now = func() time.Time { return e.now }
	c.started = e.now.Add(-time.Hour)
	c.StartupGrace = 0
	c.DisableReadyCache = true
	c.HeartbeatInterval = 10 * time.Second
	e.c = c
}

func TestParseUsage(t *testing.T) {
	for name, tc := range map[string]struct {
		in   string
		want Usage
		bad  bool
	}{
		"complete":                   {`{"prompt_tokens":3,"completion_tokens":4,"total_tokens":9}`, Usage{3, 4, 9}, false},
		"unknown fields are fine":    {`{"total_tokens":9,"cost_usd":0.1,"details":{"x":[1]}}`, Usage{0, 0, 9}, false},
		"total is derived":           {`{"prompt_tokens":3,"completion_tokens":4}`, Usage{3, 4, 7}, false},
		"total is never below parts": {`{"prompt_tokens":3,"completion_tokens":4,"total_tokens":2}`, Usage{3, 4, 7}, false},
		"empty object":               {`{}`, Usage{}, false},
		"negative":                   {`{"total_tokens":-1}`, Usage{}, true},
		"absurd":                     {`{"total_tokens":9007199254740993000}`, Usage{}, true},
		"just over the cap":          {`{"prompt_tokens":1125899906842625}`, Usage{}, true},
		"at the cap":                 {`{"total_tokens":1125899906842624}`, Usage{0, 0, 1 << 50}, false},
		"float":                      {`{"total_tokens":1.5}`, Usage{}, true},
		"string":                     {`{"total_tokens":"9"}`, Usage{}, true},
		"not an object":              {`"lots"`, Usage{}, true},
		"array":                      {`[1,2,3]`, Usage{}, true},
	} {
		got, err := parseUsage(json.RawMessage(tc.in))
		if (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
			t.Errorf("%s: got %+v err=%v, want %+v bad=%v", name, got, err, tc.want, tc.bad)
		}
	}
}

func TestHeartbeatRecordsUsageAndAttributesOnlyTheIncrease(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.report(usageJSON(10, 5, 15))
	if got := e.get().Usage; got != (Usage{10, 5, 15}) {
		t.Fatalf("usage not recorded: %+v", got)
	}
	e.report(usageJSON(10, 5, 15)) // unchanged
	e.report(usageJSON(30, 12, 42))
	m := scrape(t, e.c)
	if m[`nc_tokens_total{profile="agent",kind="prompt"}`] != 30 || m[`nc_tokens_total{profile="agent",kind="completion"}`] != 12 {
		t.Fatalf("increments were not attributed exactly once: %v", m)
	}
	for line := range m {
		if strings.HasPrefix(line, "nc_tokens_total") || strings.HasPrefix(line, "nc_budget_suspensions_total") {
			if strings.Contains(line, "a1") || strings.Contains(line, "workspace") {
				t.Fatalf("a usage metric is labelled by workspace, which is unbounded: %s", line)
			}
		}
	}
}

func TestUsageNeverDecreasesAndARegressionIsAuditedOncePerEpisode(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.report(usageJSON(60, 40, 100))
	for i := 0; i < 3; i++ { // a workload which lost its volume starts again from a small number
		e.report(usageJSON(2, 1, 3))
	}
	if got := e.get().Usage; got != (Usage{60, 40, 100}) {
		t.Fatalf("the stored usage went down: %+v", got)
	}
	if n := e.count(ActionUsageRegressed); n != 1 {
		t.Fatalf("a standing regression must be audited once, got %d", n)
	}
	m := scrape(t, e.c)
	if m[`nc_tokens_total{profile="agent",kind="prompt"}`] != 60 {
		t.Fatalf("tokens below the recorded total must not be counted again: %v", m)
	}
	e.report(usageJSON(70, 50, 120)) // the workload has caught up and passed the recorded value
	if got := e.get().Usage; got != (Usage{70, 50, 120}) {
		t.Fatalf("a report above the stored value must be taken: %+v", got)
	}
	e.report(usageJSON(1, 1, 2))
	if n := e.count(ActionUsageRegressed); n != 2 {
		t.Fatalf("a new regression after recovery is a new episode, got %d events", n)
	}
}

func TestMalformedUsageIsIgnoredWithoutHidingTheRestOfTheReport(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.report(usageJSON(10, 5, 15))
	for i, bad := range []string{`"lots"`, `{"total_tokens":-1}`, `{"total_tokens":"9"}`, `{"total_tokens":99999999999999999999}`} {
		version := fmt.Sprintf("v%d", i+2)
		e.w.version.Store(version)
		e.report(bad)
		got := e.get()
		if got.Usage != (Usage{10, 5, 15}) {
			t.Fatalf("%s changed the usage: %+v", bad, got.Usage)
		}
		if got.AgentVersion != version || got.Unresponsive {
			t.Fatalf("%s: the rest of the heartbeat was lost: %+v", bad, got)
		}
	}
	if n := e.count(ActionUsageRegressed); n != 0 {
		t.Fatalf("a rejected report is not a regression: %d", n)
	}
}

func TestWorkloadsWithoutUsageBehaveAsBefore(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.setBudget(1) // a budget on a workload which never reports can never be crossed
	for i := 0; i < 5; i++ {
		e.beat()
	}
	w := e.reconcile()
	if w.Desired != DesiredRunning || w.Usage != (Usage{}) {
		t.Fatalf("%+v", w)
	}
	b, _ := json.Marshal(w)
	if strings.Contains(string(b), `"usage"`) {
		t.Fatalf("a workspace which never reported must not show usage: %s", b)
	}
	if n := e.count(ActionBudgetExceeded); n != 0 {
		t.Fatalf("%d", n)
	}
}

func TestUsageIsFlushedAtMostOncePerIntervalButAlwaysVisible(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.c.ActivityFlushInterval = time.Hour
	e.report(usageJSON(0, 0, 10))
	e.report(usageJSON(0, 0, 20))
	e.report(usageJSON(0, 0, 30))
	if got := e.persisted().Usage.TotalTokens; got != 10 {
		t.Fatalf("a healthy workspace must not cost a write per beat, persisted %d", got)
	}
	if got := e.get().Usage.TotalTokens; got != 30 {
		t.Fatalf("readers must still see the newest value, got %d", got)
	}
	if err := e.c.FlushActivity(); err != nil {
		t.Fatal(err)
	}
	if got := e.persisted().Usage.TotalTokens; got != 30 {
		t.Fatalf("shutdown must flush usage, persisted %d", got)
	}
	e.report(usageJSON(0, 0, 40))
	e.now = e.now.Add(2 * time.Hour)
	e.report(usageJSON(0, 0, 50))
	if got := e.persisted().Usage.TotalTokens; got != 50 {
		t.Fatalf("the interval elapsed, persisted %d", got)
	}
}

func TestCrossingTheBudgetSuspendsAtOnceEvenInsideTheFlushInterval(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.c.ActivityFlushInterval = time.Hour
	e.setBudget(100)
	e.report(usageJSON(30, 20, 50))
	e.report(usageJSON(30, 20, 99))
	if w := e.persisted(); w.Desired != DesiredRunning {
		t.Fatalf("below the budget: %+v", w)
	}
	e.report(usageJSON(60, 40, 100))
	w := e.persisted() // the store, not the overlay: this is what survives a crash
	if w.Desired != DesiredSuspended || w.SuspendedFor != SuspendedForBudget || w.SuspendedAt.IsZero() || w.Usage.TotalTokens != 100 {
		t.Fatalf("crossing must be persisted immediately: %+v", w)
	}
	if !strings.Contains(w.LastError, "token budget exceeded") {
		t.Fatalf("the reason must be visible: %q", w.LastError)
	}

	w = e.reconcile()
	if w.Phase != PhaseSuspended || !strings.Contains(w.LastError, "token budget exceeded") {
		t.Fatalf("phase and reason after the workload was stopped: %s %q", w.Phase, w.LastError)
	}
	if _, stops, deletes, _ := e.r.counts(); stops == 0 || deletes != 0 {
		t.Fatalf("the workload must stop and the volume must stay: stops=%d deletes=%d", stops, deletes)
	}
	if n := e.count(ActionBudgetExceeded); n != 1 {
		t.Fatalf("one audit event for the crossing, got %d", n)
	}
	events, _ := e.c.Audit(AuditQuery{Workspace: "a1", Action: ActionBudgetExceeded})
	if events[0].Actor != ActorSystem || !strings.Contains(events[0].Detail, "token budget exceeded") {
		t.Fatalf("%+v", events[0])
	}
	hits := e.w.hits.Load()
	e.beat()
	e.reconcile()
	if e.w.hits.Load() != hits || e.count(ActionBudgetExceeded) != 1 {
		t.Fatal("a suspended workspace is not polled and not suspended twice")
	}
	if got := scrape(t, e.c)[`nc_budget_suspensions_total{profile="agent"}`]; got != 1 {
		t.Fatalf("suspensions per profile: %v", got)
	}
}

func TestBudgetOfZeroMeansUnlimited(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.report(usageJSON(1<<40, 1<<40, 1<<41))
	if w := e.reconcile(); w.Desired != DesiredRunning {
		t.Fatalf("%+v", w)
	}
}

func suspendedForBudget(t *testing.T) *beatEnv {
	t.Helper()
	e := newBeatEnv(t, "/heartbeat")
	e.setBudget(100)
	e.report(usageJSON(60, 40, 100))
	if w := e.reconcile(); w.Phase != PhaseSuspended {
		t.Fatalf("%+v", w)
	}
	return e
}

func TestBudgetSuspendedWorkspaceCannotBeWoken(t *testing.T) {
	e := suspendedForBudget(t)
	ctx := context.Background()
	ensuresBefore, _, _, restartsBefore := e.r.counts()
	if _, _, err := e.c.Acquire(ctx, "a1"); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("gateway traffic must be refused with the budget error, got %v", err)
	}
	if _, err := e.c.SetDesired(testActor, "a1", DesiredRunning); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("start: %v", err)
	}
	if _, err := e.c.SetDesired(testActor, "a1", DesiredStopped); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("stop must not quietly turn the suspension into a plain stop: %v", err)
	}
	if _, err := e.c.Restart(testActor, "a1"); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("restart: %v", err)
	}
	if _, err := e.c.Lease(testActor, "a1", "", time.Minute); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("lease: %v", err)
	}
	if errors.Is(ErrBudgetExceeded, ErrExpired) {
		t.Fatal("the two refusals must be distinguishable")
	}
	// A new deadline renews a lease, not a budget.
	if w, err := e.c.SetExpiry(testActor, "a1", e.now.Add(time.Hour)); err != nil || w.Desired != DesiredSuspended {
		t.Fatalf("a lease renewal must not lift a budget suspension: %+v %v", w, err)
	}
	if _, _, err := e.c.Acquire(ctx, "a1"); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("still refused after the renewal: %v", err)
	}
	w := e.reconcile()
	if ensures, _, _, restarts := e.r.counts(); ensures != ensuresBefore || restarts != restartsBefore || w.Desired != DesiredSuspended || w.Phase != PhaseSuspended {
		t.Fatalf("refused requests started the workload: ensures=%d restarts=%d %+v", ensures, restarts, w)
	}
}

func TestRaisingOrClearingTheBudgetBringsTheWorkspaceBack(t *testing.T) {
	for name, next := range map[string]int64{"raise": 500, "clear": 0} {
		t.Run(name, func(t *testing.T) {
			e := suspendedForBudget(t)
			w := e.setBudget(next)
			if w.Desired != DesiredStopped || w.SuspendedFor != "" || w.LastError != "" || !w.SuspendedAt.IsZero() {
				t.Fatalf("not resumed: %+v", w)
			}
			if e.count(ActionResume) != 1 {
				t.Fatalf("the resume must be audited: %d", e.count(ActionResume))
			}
			endpoint, release, err := e.c.Acquire(context.Background(), "a1")
			if err != nil || endpoint == "" {
				t.Fatalf("a resumed workspace must wake on traffic: %q %v", endpoint, err)
			}
			release()
			if w := e.get(); w.Desired != DesiredRunning || w.Phase != PhaseRunning {
				t.Fatalf("%+v", w)
			}
		})
	}
}

func TestRaisingTheBudgetBelowCurrentUsageKeepsItSuspended(t *testing.T) {
	e := suspendedForBudget(t)
	if w := e.setBudget(100); w.Desired != DesiredSuspended {
		t.Fatalf("usage 100 is still at the budget 100: %+v", w)
	}
	if w := e.setBudget(101); w.Desired != DesiredStopped {
		t.Fatalf("one token of headroom is enough: %+v", w)
	}
}

func TestLoweringTheBudgetToCurrentUsageSuspendsAtOnce(t *testing.T) {
	for _, from := range []string{DesiredRunning, DesiredStopped} {
		t.Run(from, func(t *testing.T) {
			e := newBeatEnv(t, "/heartbeat")
			e.report(usageJSON(60, 40, 100))
			if from == DesiredStopped {
				if _, err := e.c.SetDesired(testActor, "a1", DesiredStopped); err != nil {
					t.Fatal(err)
				}
			}
			w := e.setBudget(100)
			if w.Desired != DesiredSuspended || w.SuspendedFor != SuspendedForBudget {
				t.Fatalf("%+v", w)
			}
			if e.count(ActionBudgetExceeded) != 1 || e.count(ActionBudgetSet) != 1 {
				t.Fatalf("audit: exceeded=%d set=%d", e.count(ActionBudgetExceeded), e.count(ActionBudgetSet))
			}
		})
	}
}

func TestBudgetSuspensionIsNotHardDeletedByTheGracePeriod(t *testing.T) {
	e := suspendedForBudget(t)
	e.c.GracePeriod = time.Hour
	e.now = e.now.Add(48 * time.Hour)
	w := e.reconcile()
	if w.Desired != DesiredSuspended || w.Phase != PhaseSuspended {
		t.Fatalf("a budget suspension must wait for a person: %+v", w)
	}
	if _, _, deletes, _ := e.r.counts(); deletes != 0 {
		t.Fatalf("the volume was deleted: %d", deletes)
	}
}

func TestLiftingTheBudgetOnAnExpiredWorkspaceLeavesItSuspendedForTheLease(t *testing.T) {
	e := suspendedForBudget(t)
	e.c.GracePeriod = time.Hour
	if _, err := e.c.SetExpiry(testActor, "a1", e.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	e.now = e.now.Add(time.Hour)
	w := e.setBudget(0)
	if w.Desired != DesiredSuspended || w.SuspendedFor != "" || !w.SuspendedAt.Equal(e.now) {
		t.Fatalf("it is suspended for its lease now, with a fresh grace period: %+v", w)
	}
	if _, _, err := e.c.Acquire(context.Background(), "a1"); !errors.Is(err, ErrExpired) {
		t.Fatalf("got %v", err)
	}
}

func TestBudgetSuspensionSurvivesAControllerRestart(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.c.ActivityFlushInterval = time.Hour
	e.setBudget(100)
	e.report(usageJSON(60, 40, 100))
	// The controller dies after it persisted the suspension and before it
	// stopped the workload.
	if _, stops, _, _ := e.r.counts(); stops != 0 {
		t.Fatalf("setup: the workload must not have been stopped yet: %d", stops)
	}
	e.restartController()

	w := e.get()
	if w.Desired != DesiredSuspended || w.SuspendedFor != SuspendedForBudget || w.TokenBudget != 100 || w.Usage.TotalTokens != 100 {
		t.Fatalf("the suspension, budget or usage was lost: %+v", w)
	}
	if _, _, err := e.c.Acquire(context.Background(), "a1"); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("traffic after the restart: %v", err)
	}
	w = e.reconcile()
	if w.Phase != PhaseSuspended {
		t.Fatalf("the new controller must finish the stop: %+v", w)
	}
	if _, stops, _, _ := e.r.counts(); stops == 0 {
		t.Fatal("the workload was not stopped")
	}
	hits := e.w.hits.Load()
	e.beat()
	e.reconcile()
	if e.w.hits.Load() != hits {
		t.Fatal("the restarted controller polled a suspended workspace")
	}
	if n := e.count(ActionBudgetExceeded); n != 1 {
		t.Fatalf("the crossing must not be decided and audited again after a restart: %d", n)
	}
	if _, ok := scrape(t, e.c)[`nc_budget_suspensions_total{profile="agent"}`]; ok {
		t.Fatal("the restarted process counted a suspension it did not make")
	}
}

func TestSetTokenBudgetValidation(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	for _, n := range []int64{-1, maxUsageTokens + 1} {
		if _, err := e.c.SetTokenBudget(testActor, "a1", n); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%d: %v", n, err)
		}
	}
	if _, err := e.c.SetTokenBudget(testActor, "nope", 5); !errors.Is(err, ErrNotFound) {
		t.Fatalf("%v", err)
	}
	if _, err := e.c.SetDesired(testActor, "a1", DesiredDeleted); err != nil {
		t.Fatal(err)
	}
	if _, err := e.c.SetTokenBudget(testActor, "a1", 5); !errors.Is(err, ErrConflict) {
		t.Fatalf("%v", err)
	}
}
