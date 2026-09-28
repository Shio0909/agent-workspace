package control

import (
	"context"
	"errors"
	"testing"
	"time"
)

type eventRuntime struct {
	*fakeRuntime
	events chan string
}

func (r *eventRuntime) WorkspaceEvents() <-chan string { return r.events }

type failingObserveRuntime struct{ *eventRuntime }

func (r *failingObserveRuntime) Observe(context.Context, Workspace, Profile) (Observation, error) {
	return Observation{}, errors.New("observe failed")
}

func newEventTestController(t *testing.T, runtime Runtime) *Controller {
	t.Helper()
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	c := New(store, runtime, map[string]Profile{"demo": {}}, time.Hour)
	c.PollInterval = time.Hour
	c.started = time.Now().Add(-time.Hour)
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	return c
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}

func TestEventQueueDeduplicatesBacksOffAndForgets(t *testing.T) {
	q := newEventQueue()
	q.Add("demo")
	q.Add("demo")
	if got := q.pendingLen(); got != 1 {
		t.Fatalf("duplicate events left %d pending items, want 1", got)
	}
	id, ok := q.Get()
	if !ok || id != "demo" {
		t.Fatalf("Get()=%q,%v, want demo,true", id, ok)
	}

	before := time.Now()
	q.AddRateLimited("demo")
	q.mu.Lock()
	failures := q.failures["demo"]
	notBefore := q.pending["demo"]
	q.mu.Unlock()
	if failures != 1 {
		t.Fatalf("failure count=%d, want 1", failures)
	}
	if delay := notBefore.Sub(before); delay < eventRetryBase/2 || delay > eventRetryBase*2 {
		t.Fatalf("first retry delay=%s, want about %s", delay, eventRetryBase)
	}

	q.Add("demo")
	q.mu.Lock()
	accelerated := q.pending["demo"]
	q.mu.Unlock()
	if accelerated.After(time.Now()) {
		t.Fatalf("new event did not accelerate retry: %s", accelerated)
	}
	q.Forget("demo")
	q.mu.Lock()
	_, failed := q.failures["demo"]
	q.mu.Unlock()
	if failed {
		t.Fatal("Forget left the failure counter behind")
	}

	q.Shutdown()
	if _, ok := q.Get(); ok {
		t.Fatal("Get returned an item after Shutdown")
	}
}

func TestEventQueueShutdownWakesWaiter(t *testing.T) {
	q := newEventQueue()
	done := make(chan bool, 1)
	go func() {
		_, ok := q.Get()
		done <- ok
	}()
	q.Shutdown()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("blocked Get returned an item after Shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not wake blocked Get")
	}
}

func TestWakeDoesNotWaitForSlotLock(t *testing.T) {
	c := newEventTestController(t, &fakeRuntime{})
	s := c.slot("demo")
	s.mu.Lock()
	defer s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		c.wake("demo")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("wake blocked on a slot lock held by a slow operation")
	}
}

func TestStartEventsReconcilesRuntimeEvents(t *testing.T) {
	r := &eventRuntime{fakeRuntime: &fakeRuntime{running: true}, events: make(chan string, 1)}
	c := newEventTestController(t, r)
	w, err := c.store.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	w.Desired = DesiredRunning
	if err := c.store.Put(w); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.StartEvents(ctx)
		close(done)
	}()
	r.events <- "demo"
	waitUntil(t, time.Second, func() bool {
		got, err := c.Get("demo")
		return err == nil && got.Phase == PhaseRunning && r.observeCount() > 0
	}, "runtime event did not reconcile the workspace")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StartEvents did not stop after cancellation")
	}
}

func TestAcquireIsWokenByEventReconcile(t *testing.T) {
	r := &eventRuntime{fakeRuntime: &fakeRuntime{}, events: make(chan string, 1)}
	c := newEventTestController(t, r)
	ctx, cancel := context.WithCancel(context.Background())
	eventsDone := make(chan struct{})
	go func() {
		c.StartEvents(ctx)
		close(eventsDone)
	}()

	type result struct {
		endpoint string
		release  func()
		err      error
	}
	results := make(chan result, 1)
	go func() {
		endpoint, release, err := c.Acquire(ctx, "demo")
		results <- result{endpoint: endpoint, release: release, err: err}
	}()

	select {
	case got := <-results:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.endpoint == "" {
			t.Fatal("Acquire returned an empty endpoint")
		}
		got.release()
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire waited for the periodic poll instead of the event wake")
	}
	if got := c.Metrics.EventReconciles.Load(); got == 0 {
		t.Fatal("event reconcile was not counted")
	}
	cancel()
	select {
	case <-eventsDone:
	case <-time.After(time.Second):
		t.Fatal("StartEvents did not stop after cancellation")
	}
}

func TestEventWorkerRequeuesFailures(t *testing.T) {
	r := &failingObserveRuntime{eventRuntime: &eventRuntime{fakeRuntime: &fakeRuntime{}, events: make(chan string, 1)}}
	c := newEventTestController(t, r)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.StartEvents(ctx)
		close(done)
	}()
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { return c.Metrics.EventReconcileErrors.Load() > 0 }, "event failure was not retried")
	c.queue.mu.Lock()
	failures := c.queue.failures["demo"]
	c.queue.mu.Unlock()
	if failures == 0 {
		t.Fatal("failed event was not rate limited")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StartEvents did not stop after cancellation")
	}
}
