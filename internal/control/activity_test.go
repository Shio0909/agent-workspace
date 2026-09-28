package control

import (
	"context"
	"testing"
	"time"
)

func persistedActivity(t *testing.T, c *Controller, id string) time.Time {
	t.Helper()
	w, err := c.store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return w.LastActivity
}

func TestWarmRequestsDoNotPersistActivityEveryTime(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	acquire := func() {
		t.Helper()
		_, release, err := c.Acquire(context.Background(), "demo")
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	acquire() // wakes the workspace: a durable intent change
	woke := persistedActivity(t, c, "demo")
	// Stay inside the ready cache: a revalidating reconcile persists anyway.
	for i := 0; i < 40; i++ {
		now = now.Add(100 * time.Millisecond)
		acquire()
	}
	if got := persistedActivity(t, c, "demo"); !got.Equal(woke) {
		t.Fatalf("4s of warm traffic persisted activity: %v -> %v", woke, got)
	}
	if w, _ := c.Get("demo"); !w.LastActivity.Equal(now) {
		t.Fatalf("Get hides in-memory activity: %v, want %v", w.LastActivity, now)
	}
	now = woke.Add(c.ActivityFlushInterval)
	acquire()
	if got := persistedActivity(t, c, "demo"); !got.Equal(now) {
		t.Fatalf("activity not flushed after the interval: %v, want %v", got, now)
	}
}

func TestUnflushedActivityStillBlocksIdleStop(t *testing.T) {
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started = now.Add(-time.Hour)
	c.ActivityFlushInterval = time.Hour // never flush on its own
	_, release, err := c.Acquire(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	release()
	// Keep the workspace busy for longer than the idle timeout without a flush.
	for i := 0; i < 12; i++ {
		now = now.Add(10 * time.Second)
		_, release, err := c.Acquire(context.Background(), "demo")
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	now = now.Add(30 * time.Second)
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if _, stops, _, _ := r.counts(); stops != 0 {
		t.Fatal("idle stop ignored in-memory activity")
	}
	now = now.Add(time.Minute)
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if _, stops, _, _ := r.counts(); stops != 1 {
		t.Fatal("idle workspace was not stopped")
	}
}

func TestSchedulerFlushesActivityOfBusyWorkspaces(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	_, release, err := c.Acquire(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(c.ActivityFlushInterval + time.Second)
	release() // flushes because the interval has passed
	before := persistedActivity(t, c, "demo")
	_, release, err = c.Acquire(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(c.ActivityFlushInterval + time.Second)
	// The request is still open; only the scheduler can flush now.
	c.slot("demo").mu.Lock()
	c.slot("demo").activity = now
	c.slot("demo").mu.Unlock()
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if got := persistedActivity(t, c, "demo"); !got.After(before) {
		t.Fatal("scheduler round did not flush activity")
	}
	release()
}

func TestFlushActivityPersistsOnShutdown(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := &fakeRuntime{}
	c := New(s, r, map[string]Profile{"demo": {}}, time.Minute)
	c.PollInterval = time.Millisecond
	now := time.Now()
	c.now = func() time.Time { return now }
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	_, release, err := c.Acquire(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(3 * time.Second)
	release()
	if err := c.FlushActivity(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w, _ := s.Get("demo")
	if !w.LastActivity.Equal(now) {
		t.Fatalf("shutdown flush lost activity: %v, want %v", w.LastActivity, now)
	}
}

func TestZeroFlushIntervalPersistsEveryRequest(t *testing.T) {
	c, _ := fixture(t)
	c.ActivityFlushInterval = 0
	now := time.Now()
	c.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		now = now.Add(time.Millisecond)
		_, release, err := c.Acquire(context.Background(), "demo")
		if err != nil {
			t.Fatal(err)
		}
		release()
		if got := persistedActivity(t, c, "demo"); !got.Equal(now) {
			t.Fatalf("request %d not persisted", i)
		}
	}
}
