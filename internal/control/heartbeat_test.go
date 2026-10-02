package control

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// workload is a stand-in for the agent's /heartbeat endpoint.
type workload struct {
	srv     *httptest.Server
	status  atomic.Int64
	busy    atomic.Bool
	version atomic.Value
	hits    atomic.Int64
	block   chan struct{}
}

func newWorkload(t *testing.T) *workload {
	t.Helper()
	w := &workload{}
	w.status.Store(http.StatusOK)
	w.version.Store("v1")
	w.srv = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.hits.Add(1)
		if w.block != nil {
			select {
			case <-w.block:
			case <-r.Context().Done():
			}
		}
		if code := int(w.status.Load()); code != http.StatusOK {
			rw.WriteHeader(code)
			return
		}
		_, _ = rw.Write([]byte(`{"busy":` + map[bool]string{true: "true", false: "false"}[w.busy.Load()] + `,"version":"` + w.version.Load().(string) + `"}`))
	}))
	t.Cleanup(w.srv.Close)
	return w
}

// beatRuntime reports the workload's real address and lets a test make it
// not ready, which is what a hung process looks like to the kubelet.
type beatRuntime struct {
	fakeRuntime
	url      string
	notReady atomic.Bool
}

func (b *beatRuntime) Observe(context.Context, Workspace, Profile) (Observation, error) {
	ready := !b.notReady.Load()
	return Observation{Exists: true, Ready: ready, Endpoint: b.url}, nil
}

type beatEnv struct {
	t   *testing.T
	c   *Controller
	r   *beatRuntime
	w   *workload
	now time.Time
}

func newBeatEnv(t *testing.T, path string) *beatEnv {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := &beatRuntime{}
	c := New(s, r, map[string]Profile{"agent": {HeartbeatPath: path}}, 24*time.Hour)
	e := &beatEnv{t: t, c: c, r: r, w: newWorkload(t), now: time.Now()}
	r.url = e.w.srv.URL
	c.now = func() time.Time { return e.now }
	c.started = e.now.Add(-time.Hour)
	c.StartupGrace = 0
	c.DisableReadyCache = true
	c.HeartbeatInterval = 10 * time.Second
	c.HeartbeatMisses = 3
	c.RestartCooldown = 5 * time.Minute
	if _, err := c.Create(testActor, "a1", "agent"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "a1", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *beatEnv) get() Workspace {
	e.t.Helper()
	w, err := e.c.Get("a1")
	if err != nil {
		e.t.Fatal(err)
	}
	return w
}

// beat advances the clock past the poll interval and polls once.
func (e *beatEnv) beat() {
	e.now = e.now.Add(11 * time.Second)
	e.c.Beat(context.Background(), "a1", e.w.srv.URL)
}

func (e *beatEnv) actions() string {
	events, err := e.c.Audit(AuditQuery{Workspace: "a1"})
	if err != nil {
		e.t.Fatal(err)
	}
	var out []string
	for _, ev := range events {
		out = append(out, ev.Action)
	}
	return strings.Join(out, ",")
}

func TestBusyHeartbeatKeepsAWorkspaceFromGoingIdle(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.c.IdleTimeout = time.Minute
	e.w.busy.Store(true)
	for i := 0; i < 10; i++ { // 110 s, well past the one-minute idle timeout
		e.beat()
		if _, err := e.c.Reconcile(context.Background(), "a1"); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.get(); got.Desired != DesiredRunning {
		t.Fatalf("a busy workload was stopped as idle: %+v", got)
	}
	e.w.busy.Store(false)
	for i := 0; i < 10; i++ {
		e.beat()
		if _, err := e.c.Reconcile(context.Background(), "a1"); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.get(); got.Desired != DesiredStopped {
		t.Fatalf("an idle workload must still be reclaimed: %+v", got)
	}
}

func TestHeartbeatRecordsVersionAndTime(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.beat()
	w := e.get()
	if w.AgentVersion != "v1" || w.HeartbeatAt.IsZero() {
		t.Fatalf("heartbeat not recorded: %+v", w)
	}
	e.w.version.Store("v2")
	e.beat()
	if got := e.get(); got.AgentVersion != "v2" {
		t.Fatalf("version change not picked up: %q", got.AgentVersion)
	}
}

func TestLostHeartbeatRestartsOnceAndRecovers(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.beat()
	e.w.status.Store(http.StatusServiceUnavailable)
	e.beat()
	e.beat()
	if got := e.get(); got.Unresponsive || got.RestartPending {
		t.Fatalf("two misses are below the threshold of three: %+v", got)
	}
	e.beat()
	w := e.get()
	if !w.Unresponsive || !w.RestartPending {
		t.Fatalf("three misses must mark it unresponsive and request a restart: %+v", w)
	}
	if _, err := e.c.Reconcile(context.Background(), "a1"); err != nil {
		t.Fatal(err)
	}
	if e.r.restarts != 1 {
		t.Fatalf("the requested restart did not happen: %d", e.r.restarts)
	}
	// Still failing: no restart loop inside the cooldown.
	for i := 0; i < 12; i++ {
		e.beat()
		if _, err := e.c.Reconcile(context.Background(), "a1"); err != nil {
			t.Fatal(err)
		}
	}
	if e.r.restarts != 1 {
		t.Fatalf("restart repeated inside the cooldown: %d", e.r.restarts)
	}
	// After the cooldown it may try again.
	e.now = e.now.Add(6 * time.Minute)
	for i := 0; i < 3; i++ {
		e.beat()
	}
	if _, err := e.c.Reconcile(context.Background(), "a1"); err != nil {
		t.Fatal(err)
	}
	if e.r.restarts != 2 {
		t.Fatalf("expected a second restart after the cooldown: %d", e.r.restarts)
	}
	e.w.status.Store(http.StatusOK)
	e.beat()
	if got := e.get(); got.Unresponsive {
		t.Fatalf("a successful beat must clear the flag: %+v", got)
	}
	acts := e.actions()
	for _, want := range []string{ActionHeartbeatLost, ActionHeartbeatRestart, ActionHeartbeatBack} {
		if !strings.Contains(acts, want) {
			t.Fatalf("audit is missing %s: %s", want, acts)
		}
	}
	if strings.Count(acts, ActionHeartbeatLost) != 1 {
		t.Fatalf("the loss must be audited once per episode: %s", acts)
	}
}

func TestSuccessBetweenFailuresResetsTheCount(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	for i := 0; i < 5; i++ {
		e.w.status.Store(http.StatusBadGateway)
		e.beat()
		e.beat()
		e.w.status.Store(http.StatusOK)
		e.beat()
	}
	if got := e.get(); got.Unresponsive || got.RestartPending {
		t.Fatalf("failures that are not consecutive must not accumulate: %+v", got)
	}
}

func TestMissingHeartbeatPathIsAConfigurationErrorNotASickWorkload(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.w.status.Store(http.StatusNotFound)
	for i := 0; i < 10; i++ {
		e.beat()
	}
	if got := e.get(); got.Unresponsive || got.RestartPending {
		t.Fatalf("a 404 must never restart the workload: %+v", got)
	}
	if e.c.Metrics.HeartbeatFailures.Load() == 0 {
		t.Fatal("the rejected path should still be counted")
	}
}

func TestUnreachableWorkloadCountsAsMissed(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.w.srv.Close()
	for i := 0; i < 3; i++ {
		e.beat()
	}
	if got := e.get(); !got.Unresponsive {
		t.Fatalf("connection errors must count as misses: %+v", got)
	}
}

func TestHeartbeatIsRateLimitedAndOptIn(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.c.Beat(context.Background(), "a1", e.w.srv.URL)
	e.c.Beat(context.Background(), "a1", e.w.srv.URL)
	if e.w.hits.Load() != 1 {
		t.Fatalf("two calls inside the interval must poll once, got %d", e.w.hits.Load())
	}
	off := newBeatEnv(t, "")
	off.beat()
	if off.w.hits.Load() != 0 {
		t.Fatal("a profile without heartbeat_path must not be polled")
	}
}

func TestStoppedWorkspaceIsNotPolled(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	if _, err := e.c.SetDesired(testActor, "a1", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	e.beat()
	if e.w.hits.Load() != 0 {
		t.Fatal("only running workspaces are polled")
	}
}

func TestHungWorkloadDoesNotBlockTheWorkspaceLock(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.w.block = make(chan struct{})
	defer close(e.w.block)
	e.now = e.now.Add(11 * time.Second)
	done := make(chan struct{})
	go func() {
		e.c.Beat(context.Background(), "a1", e.w.srv.URL)
		close(done)
	}()
	for e.w.hits.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	locked := make(chan struct{})
	go func() {
		// Any request-path operation takes the same workspace lock.
		_, _ = e.c.SetDesired(testActor, "a1", DesiredRunning)
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		t.Fatal("a hung heartbeat held the workspace lock")
	}
	e.w.block <- struct{}{}
	<-done
}

// notReadyBeat polls the way the scheduler does when Reconcile found the
// workspace not ready: no endpoint is passed in.
func (e *beatEnv) notReadyBeat() {
	e.now = e.now.Add(11 * time.Second)
	e.c.Beat(context.Background(), "a1", "")
}

func TestHungWorkloadIsRestartedEvenThoughItIsNoLongerReady(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.beat() // healthy once
	e.r.notReady.Store(true)
	e.w.status.Store(http.StatusServiceUnavailable)
	for i := 0; i < 3; i++ {
		e.notReadyBeat()
	}
	w := e.get()
	if !w.Unresponsive || !w.RestartPending {
		t.Fatalf("a workload that was healthy and then went silent must be restarted: %+v", w)
	}
	if !w.HeartbeatAt.IsZero() {
		t.Fatal("the restart must close the health window")
	}
}

func TestStartingWorkloadIsNotRestarted(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.r.notReady.Store(true)
	e.w.status.Store(http.StatusServiceUnavailable)
	for i := 0; i < 20; i++ { // a slow image pull: never healthy, never polled
		e.notReadyBeat()
	}
	if got := e.get(); got.Unresponsive || got.RestartPending || e.w.hits.Load() != 0 {
		t.Fatalf("a pod which has not come up yet is slow, not dead: %+v hits=%d", got, e.w.hits.Load())
	}
}

func TestNotReadyDuringRolloutOrAfterLongSilenceIsNotPolled(t *testing.T) {
	e := newBeatEnv(t, "/heartbeat")
	e.beat()
	hits := e.w.hits.Load()
	w := e.get()
	w.RolloutPending = true
	if err := e.c.store.Put(w); err != nil {
		t.Fatal(err)
	}
	e.r.notReady.Store(true)
	e.notReadyBeat()
	if e.w.hits.Load() != hits {
		t.Fatal("a pod being replaced by an upgrade must not be polled")
	}
	w.RolloutPending = false
	if err := e.c.store.Put(w); err != nil {
		t.Fatal(err)
	}
	e.now = e.now.Add(time.Hour) // the last success is long gone
	e.c.Beat(context.Background(), "a1", "")
	if e.w.hits.Load() != hits {
		t.Fatal("a stale health record must not be treated as recent")
	}
}
