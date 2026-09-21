package control

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// concurrencyRuntime 记录同时在飞的 Observe 数量，用来断言一轮扫描的并发上限。
type concurrencyRuntime struct {
	fakeRuntime
	inFlight     atomic.Int64
	maxInFlight  atomic.Int64
	observeCalls atomic.Int64
	delay        time.Duration
}

func (c *concurrencyRuntime) Observe(ctx context.Context, w Workspace, p Profile) (Observation, error) {
	c.observeCalls.Add(1)
	current := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	for {
		max := c.maxInFlight.Load()
		if current <= max || c.maxInFlight.CompareAndSwap(max, current) {
			break
		}
	}
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.fakeRuntime.Observe(ctx, w, p)
}

// selectiveRuntime 让指定的工作区对账失败，用来验证单条失败不影响整轮。
type selectiveRuntime struct {
	fakeRuntime
	failOn string
	calls  atomic.Int64
}

func (s *selectiveRuntime) Observe(ctx context.Context, w Workspace, p Profile) (Observation, error) {
	s.calls.Add(1)
	if w.ID == s.failOn {
		return Observation{}, errors.New("backend rejected the operation")
	}
	return s.fakeRuntime.Observe(ctx, w, p)
}

func seed(t *testing.T, c *Controller, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := c.Create(testActor, id, "demo"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSchedulerDefaultsMatchTheBoundedSweep(t *testing.T) {
	s := NewScheduler(&Controller{})
	if s.Concurrency != 4 || s.BatchSize <= 0 || s.Interval <= 0 || s.RoundTimeout <= 0 {
		t.Fatalf("unexpected defaults: %+v", s)
	}
}

func TestScanPaginatesWithoutSkippingOrRepeating(t *testing.T) {
	c, _ := fixture(t)
	seed(t, c, "w-1", "w-2", "w-3", "w-4", "w-5", "w-6", "w-7", "w-8", "w-9")
	seen := map[string]int{}
	after, pages := "", 0
	for {
		pages++
		if pages > 20 {
			t.Fatal("scan did not terminate")
		}
		page, more := c.Scan(after, 3)
		if len(page) == 0 {
			break
		}
		if len(page) > 3 {
			t.Fatalf("page larger than the limit: %d", len(page))
		}
		for i, w := range page {
			if i > 0 && page[i-1].ID >= w.ID {
				t.Fatalf("page is not ordered: %v", page)
			}
			seen[w.ID]++
			after = w.ID
		}
		if !more {
			break
		}
	}
	if len(seen) != 10 {
		t.Fatalf("scan saw %d workspaces, want 10", len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("%s was returned %d times", id, count)
		}
	}
	if page, _ := c.Scan("", 0); page != nil {
		t.Fatal("a zero limit must not return workspaces")
	}
}

func TestRoundScansInBatchesWithBoundedConcurrency(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := &concurrencyRuntime{delay: 20 * time.Millisecond}
	c := New(s, r, map[string]Profile{"demo": {}}, time.Hour)
	ids := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		ids = append(ids, fmt.Sprintf("w-%d", i))
	}
	seed(t, c, ids...)
	for _, id := range ids {
		if _, err := c.SetDesired(testActor, id, DesiredRunning); err != nil {
			t.Fatal(err)
		}
	}
	scheduler := NewScheduler(c)
	scheduler.BatchSize, scheduler.Concurrency = 3, 2
	stats := scheduler.Round(context.Background())
	if stats.Scanned != 10 || stats.Failed != 0 || stats.TimedOut {
		t.Fatalf("unexpected round: %+v", stats)
	}
	if r.observeCalls.Load() != 10 {
		t.Fatalf("not every workspace was reconciled: %d", r.observeCalls.Load())
	}
	if got := r.maxInFlight.Load(); got > 2 {
		t.Fatalf("concurrency limit exceeded: %d", got)
	} else if got < 2 {
		t.Fatal("batches were reconciled serially, so the limit was never exercised")
	}
}

func TestRoundTimeoutStopsDispatching(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := &concurrencyRuntime{delay: 30 * time.Millisecond}
	c := New(s, r, map[string]Profile{"demo": {}}, time.Hour)
	for i := 0; i < 40; i++ {
		seed(t, c, fmt.Sprintf("w-%02d", i))
	}
	scheduler := NewScheduler(c)
	scheduler.BatchSize, scheduler.Concurrency, scheduler.RoundTimeout = 8, 2, 60*time.Millisecond
	started := time.Now()
	stats := scheduler.Round(context.Background())
	if !stats.TimedOut {
		t.Fatalf("round did not report the timeout: %+v", stats)
	}
	if stats.Scanned == 0 || stats.Scanned >= 40 {
		t.Fatalf("round did not stop partway: %+v", stats)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("round ignored its deadline: %s", elapsed)
	}
}

func TestRoundContinuesAfterOneWorkspaceFails(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := &selectiveRuntime{failOn: "bad"}
	c := New(s, r, map[string]Profile{"demo": {}}, time.Hour)
	seed(t, c, "good-1", "bad", "good-2")
	for _, id := range []string{"good-1", "bad", "good-2"} {
		if _, err := c.SetDesired(testActor, id, DesiredRunning); err != nil {
			t.Fatal(err)
		}
	}
	scheduler := NewScheduler(c)
	stats := scheduler.Round(context.Background())
	if stats.Scanned != 3 || stats.Failed != 1 {
		t.Fatalf("one bad workspace changed the round: %+v", stats)
	}
	for _, id := range []string{"good-1", "good-2"} {
		if w, _ := c.Get(id); w.Phase == PhaseError || w.UpdatedAt.IsZero() {
			t.Fatalf("%s was not reconciled: %+v", id, w)
		}
	}
	if r.calls.Load() != 3 {
		t.Fatalf("round skipped a workspace: %d observations", r.calls.Load())
	}
	if w, _ := c.Get("bad"); w.Phase != PhaseError || w.LastError == "" {
		t.Fatalf("failed workspace did not record its error: %+v", w)
	}
	if c.Metrics.ReconcileErrors.Load() != 1 {
		t.Fatalf("reconcile failures were not counted: %d", c.Metrics.ReconcileErrors.Load())
	}
}

func TestRoundSkipsTombstones(t *testing.T) {
	c, _ := fixture(t)
	ctx := context.Background()
	if _, err := c.SetDesired(testActor, "demo", DesiredDeleted); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	seed(t, c, "alive")
	before := c.Metrics.Reconciles.Load()
	stats := NewScheduler(c).Round(ctx)
	if stats.Skipped != 1 || stats.Scanned != 1 {
		t.Fatalf("tombstone was not skipped: %+v", stats)
	}
	if after := c.Metrics.Reconciles.Load(); after != before+1 {
		t.Fatalf("tombstone reached Reconcile: %d -> %d", before, after)
	}
}

func TestRunTicksUntilContextIsCancelled(t *testing.T) {
	// 工作区保持 stopped：running 会被就绪端点缓存挡住，反而看不出轮询在继续。
	c, r := fixture(t)
	scheduler := NewScheduler(c)
	scheduler.Interval, scheduler.RoundTimeout = 5*time.Millisecond, time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		scheduler.Run(ctx)
	}()
	stopped := func() int {
		_, stops, _, _ := r.counts()
		return stops
	}
	deadline := time.Now().Add(2 * time.Second)
	for stopped() < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("scheduler did not tick repeatedly: %d rounds", stopped())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
