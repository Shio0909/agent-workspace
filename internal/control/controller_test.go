package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// testActor 是测试里代表"调用方"的归属名，用来断言审计记录确实来自请求方。
const testActor = "tester"

type fakeRuntime struct {
	mu                      sync.Mutex
	running                 bool
	ensures, stops, deletes int
	restarts                int
	failEnsure              bool
	failRestart             bool
	failDelete              bool
	failStop                bool
	observes                int
	probeErr                error
	// observeDelay 让并发对账的重叠可观测，用于验证并发上限。
	observeDelay time.Duration
}

func (f *fakeRuntime) Ensure(context.Context, Workspace, Profile) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensures++
	if f.failEnsure {
		return errors.New("temporary Kubernetes failure")
	}
	f.running = true
	return nil
}
func (f *fakeRuntime) Observe(context.Context, Workspace, Profile) (Observation, error) {
	if f.observeDelay > 0 {
		time.Sleep(f.observeDelay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observes++
	return Observation{Exists: f.running, Ready: f.running, Endpoint: "http://agent:8080"}, nil
}

func TestReadyEndpointReuseExpiryInvalidationAndStop(t *testing.T) {
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	acquire := func() string {
		t.Helper()
		endpoint, release, err := c.Acquire(context.Background(), "demo")
		if err != nil {
			t.Fatal(err)
		}
		release()
		return endpoint
	}
	endpoint := acquire()
	observes := r.observes
	for i := 0; i < 20; i++ {
		acquire()
	}
	if r.observes != observes {
		t.Fatal("warm requests still probe runtime")
	}
	now = now.Add(runtimeReadyTTL)
	acquire()
	if r.observes != observes+1 {
		t.Fatal("expired endpoint did not revalidate")
	}
	c.InvalidateEndpoint("demo", endpoint)
	acquire()
	if r.observes != observes+2 {
		t.Fatal("failed endpoint was reused")
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	acquire()
	if r.ensures != 2 {
		t.Fatalf("stopped runtime not restarted: ensures=%d", r.ensures)
	}
	observes = r.observes
	restarted := New(c.store, r, c.profiles, time.Minute)
	_, release, err := restarted.Acquire(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if r.observes != observes+1 {
		t.Fatal("restart trusted stale ready state")
	}
}
func (f *fakeRuntime) Stop(context.Context, Workspace) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops++
	if f.failStop {
		return errors.New("temporary Kubernetes failure")
	}
	f.running = false
	return nil
}
func (f *fakeRuntime) Delete(context.Context, Workspace) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	if f.failDelete {
		return errors.New("temporary Kubernetes failure")
	}
	f.running = false
	return nil
}

func (f *fakeRuntime) Restart(context.Context, Workspace) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restarts++
	if f.failRestart {
		return errors.New("temporary Kubernetes failure")
	}
	f.running = true
	return nil
}

func (f *fakeRuntime) Probe(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.probeErr
}

func (f *fakeRuntime) observeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.observes
}

func (f *fakeRuntime) counts() (ensures, stops, deletes, restarts int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ensures, f.stops, f.deletes, f.restarts
}

func fixture(t *testing.T) (*Controller, *fakeRuntime) {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := &fakeRuntime{}
	c := New(s, r, map[string]Profile{"demo": {}}, time.Minute)
	c.PollInterval = time.Millisecond
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	return c, r
}

func TestConcurrentAcquireSharesOneRuntimeAndBlocksStop(t *testing.T) {
	c, r := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	releases := make(chan func(), 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, err := c.Acquire(ctx, "demo")
			if err != nil {
				t.Error(err)
				return
			}
			releases <- release
		}()
	}
	wg.Wait()
	close(releases)
	if r.ensures != 1 {
		t.Fatalf("wanted one runtime start, got %d", r.ensures)
	}
	for _, desired := range []string{DesiredStopped, DesiredDeleted} {
		if _, err := c.SetDesired(testActor, "demo", desired); !errors.Is(err, ErrConflict) {
			t.Fatalf("active request allowed %s: %v", desired, err)
		}
	}
	for release := range releases {
		release()
		release()
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if r.stops != 1 || r.deletes != 0 {
		t.Fatal("stop must preserve workspace storage")
	}
}

func TestLeasePreventsIdleAndExpires(t *testing.T) {
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started = now.Add(-time.Hour)
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	lease, err := c.Lease(testActor, "demo", "", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if r.stops != 0 {
		t.Fatal("reaped an active background task")
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredDeleted); !errors.Is(err, ErrConflict) {
		t.Fatal("deleted leased workspace")
	}
	now = now.Add(9 * time.Minute)
	if _, err := c.Lease(testActor, "demo", lease, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatal("renewed expired lease")
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	w, _ := c.Get("demo")
	if w.Desired != DesiredStopped || r.stops != 1 {
		t.Fatal("expired idle workspace not stopped")
	}
}

func TestIntentAndLeaseRecoverAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := &fakeRuntime{failEnsure: true}
	profiles := map[string]Profile{"demo": {}}
	c := New(s, r, profiles, time.Minute)
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	lease, err := c.Lease(testActor, "demo", "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err == nil {
		t.Fatal("expected runtime failure")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r.failEnsure = false
	c = New(s, r, profiles, time.Minute)
	w, _ := c.Get("demo")
	if w.Desired != DesiredRunning || w.Phase != PhaseError || w.Leases[lease].IsZero() {
		t.Fatal("lost durable intent/lease")
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
			t.Fatal(err)
		}
	}
	w, _ = c.Get("demo")
	if w.Phase != PhaseRunning || w.LastError != "" {
		t.Fatal("failed operation did not recover")
	}
}

func TestRestartGraceAndRequestActivity(t *testing.T) {
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started = now
	_, release, err := c.Acquire(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if r.stops != 0 {
		t.Fatal("reaped active stream")
	}
	release()
	w, _ := c.Get("demo")
	if !w.LastActivity.Equal(now) {
		t.Fatal("idle clock did not start at stream completion")
	}
	now = now.Add(2 * time.Minute)
	c.started = now // simulate a controller restart; allow the agent to renew its lease
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if r.stops != 0 {
		t.Fatal("ignored controller restart grace")
	}
	now = now.Add(2 * time.Minute)
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if r.stops != 1 {
		t.Fatal("idle workspace did not stop")
	}
}

func TestCreateIdempotencyAndExplicitDelete(t *testing.T) {
	c, r := fixture(t)
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	if len(c.List()) != 1 {
		t.Fatal("duplicate workspace")
	}
	if _, err := c.Create(testActor, "../escape", "demo"); !errors.Is(err, ErrInvalid) {
		t.Fatal("accepted unsafe id")
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredDeleted); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if r.deletes != 1 {
		t.Fatal("explicit delete did not remove runtime")
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); !errors.Is(err, ErrConflict) {
		t.Fatal("resurrected deleted workspace")
	}
	if _, err := c.Create(testActor, "demo", "demo"); !errors.Is(err, ErrConflict) {
		t.Fatal("reused tombstoned id")
	}
}
