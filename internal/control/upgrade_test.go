package control

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	imgV1 = "reg.example/agent:v1"
	imgV2 = "reg.example/agent:v2"
	imgV3 = "reg.example/agent:v3"
)

// rolloutRuntime models what the Kubernetes runtime reports during an image
// change: the pod that is serving, and the image it runs. An image listed in
// bad never becomes ready, and hold freezes the rollout so the old pod keeps
// serving, which is the window in which Ready alone is misleading.
type rolloutRuntime struct {
	fakeRuntime
	rmu     sync.Mutex
	serving string
	bad     map[string]bool
	hold    bool
	applied []string
}

func (r *rolloutRuntime) Ensure(_ context.Context, _ Workspace, p Profile) error {
	r.rmu.Lock()
	defer r.rmu.Unlock()
	r.applied = append(r.applied, p.Image)
	switch {
	case r.serving == p.Image, r.hold:
	case r.bad[p.Image]:
		r.serving = ""
	default:
		r.serving = p.Image
	}
	return nil
}

func (r *rolloutRuntime) Observe(context.Context, Workspace, Profile) (Observation, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()
	return Observation{Exists: true, Ready: r.serving != "", Endpoint: "http://agent:8080", Image: r.serving}, nil
}

func (r *rolloutRuntime) lastApplied() string {
	r.rmu.Lock()
	defer r.rmu.Unlock()
	return r.applied[len(r.applied)-1]
}

type upgradeEnv struct {
	t   *testing.T
	c   *Controller
	r   *rolloutRuntime
	now time.Time
}

func newUpgradeEnv(t *testing.T) *upgradeEnv {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := &rolloutRuntime{bad: map[string]bool{}}
	profile := Profile{Image: imgV1, AllowedImages: []string{imgV2, imgV3, "reg.example/other/*"}}
	c := New(s, r, map[string]Profile{"agent": profile}, 24*time.Hour)
	e := &upgradeEnv{t: t, c: c, r: r, now: time.Now()}
	c.now = func() time.Time { return e.now }
	c.started = e.now.Add(-48 * time.Hour)
	c.StartupGrace = 0
	c.DisableReadyCache = true
	c.UpgradeSettle = 10 * time.Second
	if _, err := c.Create(testActor, "a1", "agent"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "a1", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	e.reconcile(2) // first pass applies v1, second sees it ready
	if w := e.get(); w.Phase != PhaseRunning {
		t.Fatalf("baseline is not running: %+v", w)
	}
	return e
}

func (e *upgradeEnv) get() Workspace {
	e.t.Helper()
	w, err := e.c.Get("a1")
	if err != nil {
		e.t.Fatal(err)
	}
	return w
}

// reconcile runs n passes, moving the fake clock one second per pass.
func (e *upgradeEnv) reconcile(n int) {
	e.t.Helper()
	for i := 0; i < n; i++ {
		if _, err := e.c.Reconcile(context.Background(), "a1"); err != nil {
			e.t.Fatal(err)
		}
		e.advance(time.Second)
	}
}

func (e *upgradeEnv) advance(d time.Duration) { e.now = e.now.Add(d) }

func (e *upgradeEnv) actions() string {
	e.t.Helper()
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

func TestUpgradeOnlyToAllowedImages(t *testing.T) {
	e := newUpgradeEnv(t)
	for _, image := range []string{"evil.example/agent:v1", "reg.example/agent:v9", "has space", "", "reg.example/other"} {
		if _, err := e.c.Upgrade(testActor, "a1", image, 0); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%q: expected ErrInvalid, got %v", image, err)
		}
	}
	if _, err := e.c.Upgrade(testActor, "a1", "reg.example/other/tool:1", 0); err != nil {
		t.Fatalf("prefix pattern must allow a matching image: %v", err)
	}
}

func TestUpgradeIsRefusedWhenProfileAllowsNothing(t *testing.T) {
	c, _ := fixture(t)
	if _, err := c.Upgrade(testActor, "demo", imgV2, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a profile with no allowed_images must refuse upgrades, got %v", err)
	}
}

func TestUpgradeTimeoutBounds(t *testing.T) {
	e := newUpgradeEnv(t)
	for _, d := range []time.Duration{time.Second, -time.Minute, 2 * time.Hour} {
		if _, err := e.c.Upgrade(testActor, "a1", imgV2, d); !errors.Is(err, ErrInvalid) {
			t.Fatalf("timeout %s: expected ErrInvalid, got %v", d, err)
		}
	}
}

func TestUpgradeCommitsAfterStayingReady(t *testing.T) {
	e := newUpgradeEnv(t)
	if _, err := e.c.Upgrade(testActor, "a1", imgV2, 0); err != nil {
		t.Fatal(err)
	}
	e.reconcile(1)
	if got := e.r.lastApplied(); got != imgV2 {
		t.Fatalf("the new image was not applied: %s", got)
	}
	e.reconcile(5) // ready on v2 but not yet for UpgradeSettle
	w := e.get()
	if w.Upgrade == nil || w.Upgrade.ReadySince.IsZero() {
		t.Fatalf("upgrade should still be settling: %+v", w.Upgrade)
	}
	e.reconcile(8)
	w = e.get()
	if w.Upgrade != nil || w.Image != imgV2 || w.PreviousImage != imgV1 {
		t.Fatalf("upgrade not committed: %+v", w)
	}
	if w.LastUpgrade == nil || w.LastUpgrade.Outcome != UpgradeCommitted {
		t.Fatalf("outcome not recorded: %+v", w.LastUpgrade)
	}
	if w.Phase != PhaseRunning {
		t.Fatalf("workspace should be running: %s", w.Phase)
	}
	if got := e.actions(); !strings.Contains(got, "upgrade,") || !strings.HasSuffix(got, "upgrade-commit") {
		t.Fatalf("audit trail: %s", got)
	}
	if e.c.Metrics.UpgradeCommits.Load() != 1 {
		t.Fatal("commit not counted")
	}
}

func TestOldPodStillReadyDoesNotCountAsUpgraded(t *testing.T) {
	e := newUpgradeEnv(t)
	e.r.hold = true // the rollout has not started: v1 keeps serving
	if _, err := e.c.Upgrade(testActor, "a1", imgV2, 0); err != nil {
		t.Fatal(err)
	}
	e.reconcile(30)
	w := e.get()
	if w.Upgrade == nil || w.Phase == PhaseRunning {
		t.Fatalf("old image must not be reported as the upgrade: %+v", w)
	}
	if w.Upgrade.ReadySince != (time.Time{}) {
		t.Fatal("ready on the old image must not start the settle clock")
	}
}

func TestFailedUpgradeRollsBackAutomatically(t *testing.T) {
	e := newUpgradeEnv(t)
	e.r.bad[imgV2] = true
	if _, err := e.c.Upgrade(testActor, "a1", imgV2, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	e.reconcile(20)
	if w := e.get(); w.Upgrade == nil || w.Phase == PhaseRunning {
		t.Fatalf("a failing image must not be running before the deadline: %+v", w)
	}
	e.reconcile(15) // past the 30s deadline
	w := e.get()
	if w.Upgrade != nil || w.Image != "" {
		t.Fatalf("expected a return to the profile image: image=%q upgrade=%+v", w.Image, w.Upgrade)
	}
	if w.LastUpgrade == nil || w.LastUpgrade.Outcome != UpgradeRolledBack || !strings.Contains(w.LastUpgrade.Reason, "not ready") {
		t.Fatalf("outcome: %+v", w.LastUpgrade)
	}
	e.reconcile(3)
	w = e.get()
	if w.Phase != PhaseRunning || e.r.lastApplied() != imgV1 || e.r.serving != imgV1 {
		t.Fatalf("workspace did not recover on v1: phase=%s applied=%s serving=%s", w.Phase, e.r.lastApplied(), e.r.serving)
	}
	if w.PreviousImage != "" {
		t.Fatalf("a failed upgrade must not become the rollback target: %q", w.PreviousImage)
	}
	if !strings.HasSuffix(e.actions(), "upgrade-rollback") || e.c.Metrics.UpgradeRollbacks.Load() != 1 {
		t.Fatalf("rollback not audited or counted: %s", e.actions())
	}
}

func TestCrashAfterBecomingReadyDoesNotCommit(t *testing.T) {
	e := newUpgradeEnv(t)
	if _, err := e.c.Upgrade(testActor, "a1", imgV2, time.Minute); err != nil {
		t.Fatal(err)
	}
	e.reconcile(5) // ready on v2 for a few seconds, less than the settle time
	e.r.rmu.Lock()
	e.r.serving = "" // the process starts, then dies
	e.r.rmu.Unlock()
	e.r.bad[imgV2] = true
	e.reconcile(1)
	if w := e.get(); w.Upgrade == nil || !w.Upgrade.ReadySince.IsZero() {
		t.Fatalf("a crash must reset the settle clock: %+v", w.Upgrade)
	}
	e.reconcile(70)
	w := e.get()
	if w.Upgrade != nil || w.LastUpgrade == nil || w.LastUpgrade.Outcome != UpgradeRolledBack {
		t.Fatalf("a crash-looping upgrade must roll back: %+v / %+v", w.Upgrade, w.LastUpgrade)
	}
}

func TestUpgradeClockWaitsWhileStopped(t *testing.T) {
	e := newUpgradeEnv(t)
	if _, err := e.c.SetDesired(testActor, "a1", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	e.reconcile(1)
	if _, err := e.c.Upgrade(testActor, "a1", imgV2, time.Minute); err != nil {
		t.Fatal(err)
	}
	e.reconcile(1)
	e.advance(48 * time.Hour) // a long weekend
	if _, err := e.c.SetDesired(testActor, "a1", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	e.reconcile(1)
	if w := e.get(); w.Upgrade == nil {
		t.Fatalf("upgrade was decided while the workspace never ran: %+v", w.LastUpgrade)
	}
	e.reconcile(20)
	if w := e.get(); w.Image != imgV2 || w.LastUpgrade == nil || w.LastUpgrade.Outcome != UpgradeCommitted {
		t.Fatalf("upgrade should commit once it has actually run: %+v", w)
	}
}

func TestUpgradeRequestsAreIdempotentOrConflict(t *testing.T) {
	e := newUpgradeEnv(t)
	first, err := e.c.Upgrade(testActor, "a1", imgV2, 0)
	if err != nil {
		t.Fatal(err)
	}
	e.advance(time.Minute)
	again, err := e.c.Upgrade(testActor, "a1", imgV2, 0)
	if err != nil || !again.Upgrade.StartedAt.Equal(first.Upgrade.StartedAt) {
		t.Fatalf("repeating the target must not restart the upgrade: %v %+v", err, again.Upgrade)
	}
	if _, err := e.c.Upgrade(testActor, "a1", imgV3, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("a different target mid-upgrade must conflict, got %v", err)
	}
	e.reconcile(20)
	if w, err := e.c.Upgrade(testActor, "a1", imgV2, 0); err != nil || w.Upgrade != nil {
		t.Fatalf("upgrading to the image already running is a no-op: %v %+v", err, w.Upgrade)
	}
}

func TestRollbackAbortsAnUpgradeInFlight(t *testing.T) {
	e := newUpgradeEnv(t)
	if _, err := e.c.Upgrade(testActor, "a1", imgV2, 0); err != nil {
		t.Fatal(err)
	}
	e.reconcile(2)
	w, err := e.c.Rollback(testActor, "a1")
	if err != nil || w.Upgrade != nil || w.Image != "" {
		t.Fatalf("rollback: %v %+v", err, w)
	}
	if w.LastUpgrade == nil || w.LastUpgrade.Outcome != UpgradeAborted {
		t.Fatalf("outcome: %+v", w.LastUpgrade)
	}
	e.reconcile(3)
	if e.r.serving != imgV1 {
		t.Fatalf("workspace did not go back to v1: %s", e.r.serving)
	}
}

func TestRollbackAfterCommitTogglesBetweenImages(t *testing.T) {
	e := newUpgradeEnv(t)
	if _, err := e.c.Rollback(testActor, "a1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("nothing to roll back to, got %v", err)
	}
	if _, err := e.c.Upgrade(testActor, "a1", imgV2, 0); err != nil {
		t.Fatal(err)
	}
	e.reconcile(20)
	if w := e.get(); w.PreviousImage != imgV1 {
		t.Fatalf("not committed: %+v", w)
	}
	w, err := e.c.Rollback(testActor, "a1")
	if err != nil || w.Image != "" || w.PreviousImage != imgV2 {
		t.Fatalf("rollback to v1: %v image=%q previous=%q", err, w.Image, w.PreviousImage)
	}
	e.reconcile(3)
	if e.r.serving != imgV1 {
		t.Fatalf("serving %s", e.r.serving)
	}
	w, err = e.c.Rollback(testActor, "a1")
	if err != nil || w.Image != imgV2 || w.PreviousImage != imgV1 {
		t.Fatalf("second rollback should go forward again: %v image=%q previous=%q", err, w.Image, w.PreviousImage)
	}
}

func TestUpgradeSurvivesControllerRestart(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Controller, *Store) {
		s, err := OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		profile := Profile{Image: imgV1, AllowedImages: []string{imgV2}}
		return New(s, &rolloutRuntime{bad: map[string]bool{}}, map[string]Profile{"agent": profile}, time.Hour), s
	}
	c, s := open()
	if _, err := c.Create(testActor, "a1", "agent"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Upgrade(testActor, "a1", imgV2, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c, s = open()
	defer s.Close()
	w, err := c.Get("a1")
	if err != nil || w.Upgrade == nil || w.Upgrade.From != imgV1 || w.Upgrade.To != imgV2 || w.Image != imgV2 {
		t.Fatalf("the fallback image must survive a restart: %v %+v", err, w)
	}
}

func TestProfileValidation(t *testing.T) {
	base := Profile{Image: "i", Port: 80, HealthPath: "/h", MountPath: "/m", Storage: "1Gi", CPU: "1", Memory: "1Gi"}
	good := base
	good.AllowedImages, good.HeartbeatPath = []string{"reg.example/a:1", "reg.example/b/*"}, "/heartbeat"
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Profile){
		"bare wildcard":      func(p *Profile) { p.AllowedImages = []string{"*"} },
		"empty entry":        func(p *Profile) { p.AllowedImages = []string{""} },
		"bad characters":     func(p *Profile) { p.AllowedImages = []string{"a b"} },
		"relative heartbeat": func(p *Profile) { p.HeartbeatPath = "heartbeat" },
	} {
		p := base
		mutate(&p)
		if err := p.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: expected ErrInvalid, got %v", name, err)
		}
	}
}

func TestImageAllowlistUsesExplicitPrefixSemantics(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		image   string
		want    bool
	}{
		{"registry/team/*", "registry/team/agent:v1", true},
		{"registry/team/*", "registry/team-evil/agent:v1", false},
		{"registry/agent:*", "registry/agent:v2", true},
		{"registry/agent:*", "registry/agent-evil:v2", false},
		{"registry/agent*", "registry/agent-evil:v2", true},
		{"registry/agent@sha256:abc", "registry/agent@sha256:abc", true},
		{"registry/agent@sha256:abc", "registry/agent@sha256:def", false},
	} {
		if got := imageAllowed(Profile{AllowedImages: []string{tc.pattern}}, tc.image); got != tc.want {
			t.Errorf("pattern=%q, image=%q: got %v, want %v", tc.pattern, tc.image, got, tc.want)
		}
	}
}
