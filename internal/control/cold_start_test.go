package control

import (
	"context"
	"sync"
	"testing"
	"time"
)

type startupRuntime struct {
	*fakeRuntime
	mu     sync.Mutex
	ts     StartupTimestamps
	direct []bool
}

func (r *startupRuntime) StartupTimestamps(_ context.Context, _ Workspace, allowDirect bool) (StartupTimestamps, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.direct = append(r.direct, allowDirect)
	if !allowDirect {
		return StartupTimestamps{}, false
	}
	return r.ts, !r.ts.Scheduled.IsZero()
}

func TestColdStartTransitionRecordsPhases(t *testing.T) {
	oldTimeout, oldInterval, oldDirect := startPhaseObservationTimeout, startPhaseObservationInterval, startPhaseDirectTimeout
	startPhaseObservationTimeout, startPhaseObservationInterval, startPhaseDirectTimeout = 40*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() {
		startPhaseObservationTimeout, startPhaseObservationInterval, startPhaseDirectTimeout = oldTimeout, oldInterval, oldDirect
	})
	r := &startupRuntime{fakeRuntime: &fakeRuntime{}}
	c := newEventTestController(t, r)
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.startedAt.Load("demo"); !ok {
		t.Fatal("starting transition did not record a cold-start start time")
	}

	t0 := time.Now().Add(-3 * time.Second)
	c.startedAt.Store("demo", t0)
	r.ts = StartupTimestamps{
		Scheduled:        t0.Add(200 * time.Millisecond),
		ContainerStarted: t0.Add(time.Second),
		Ready:            t0.Add(3 * time.Second),
		PodName:          "demo-pod",
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	var values map[string]float64
	waitUntil(t, time.Second, func() bool {
		values = scrape(t, c)
		return values[`nc_workspace_start_seconds_count{phase="ready"}`] == 1
	}, "cold-start phase metrics were not recorded")
	for _, name := range []string{
		`nc_workspace_start_seconds_count{phase="schedule"}`,
		`nc_workspace_start_seconds_count{phase="pull"}`,
		`nc_workspace_start_seconds_count{phase="ready"}`,
		`nc_workspace_start_seconds_count{phase="total"}`,
	} {
		if got := values[name]; got != 1 {
			t.Fatalf("%s=%v, want 1", name, got)
		}
	}
	if got := values[`nc_workspace_start_seconds_sum{phase="schedule"}`]; got <= 0 {
		t.Fatalf("schedule sum=%v, want a positive duration", got)
	}
	if _, ok := c.startedAt.Load("demo"); ok {
		t.Fatal("completed cold start left its start time behind")
	}
}

func TestColdStartRetryKeepsTheOriginalStartTime(t *testing.T) {
	r := &startupRuntime{fakeRuntime: &fakeRuntime{failEnsure: true}}
	c := newEventTestController(t, r)
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err == nil {
		t.Fatal("expected the injected Ensure failure")
	}
	first, ok := c.startedAt.Load("demo")
	if !ok {
		t.Fatal("failed start did not retain its start time")
	}
	time.Sleep(5 * time.Millisecond)
	r.failEnsure = false
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	second, ok := c.startedAt.Load("demo")
	if !ok || !first.(time.Time).Equal(second.(time.Time)) {
		t.Fatalf("retry reset start time: first=%v second=%v", first, second)
	}
}

func TestStartPhaseDirectFallbackOnlyRunsAtDeadline(t *testing.T) {
	oldTimeout, oldInterval, oldDirect := startPhaseObservationTimeout, startPhaseObservationInterval, startPhaseDirectTimeout
	startPhaseObservationTimeout, startPhaseObservationInterval, startPhaseDirectTimeout = 40*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() {
		startPhaseObservationTimeout, startPhaseObservationInterval, startPhaseDirectTimeout = oldTimeout, oldInterval, oldDirect
	})

	r := &startupRuntime{fakeRuntime: &fakeRuntime{}}
	c := newEventTestController(t, r)
	t0 := time.Now().Add(-time.Second)
	r.ts = StartupTimestamps{
		Scheduled:        t0.Add(100 * time.Millisecond),
		ContainerStarted: t0.Add(500 * time.Millisecond),
		Ready:            t0.Add(time.Second),
	}
	c.startedAt.Store("demo", t0)
	c.recordColdStart(context.Background(), Workspace{ID: "demo"})
	waitUntil(t, time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.direct) > 0 && r.direct[len(r.direct)-1]
	}, "direct fallback did not run")

	r.mu.Lock()
	calls := append([]bool(nil), r.direct...)
	r.mu.Unlock()
	if len(calls) < 2 || calls[0] || !calls[len(calls)-1] {
		t.Fatalf("direct-fallback calls=%v, want cache polls followed by one direct read", calls)
	}
	directCalls := 0
	for _, direct := range calls {
		if direct {
			directCalls++
		}
	}
	if directCalls != 1 {
		t.Fatalf("direct fallback ran %d times, want 1: %v", directCalls, calls)
	}
}
