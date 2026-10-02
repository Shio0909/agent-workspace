package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-workspace/internal/agent"
	"agent-workspace/internal/agent/fakellm"
)

// podRuntime is a Runtime whose workload is a real agent. It models the one
// property these tests are about: a pod is disposable and its volume is not.
// Every start builds a brand-new agent.Agent, so nothing survives in memory;
// only the directory handed to it does, and only Delete removes that.
type podRuntime struct {
	mu       sync.Mutex
	volume   string
	creds    string
	llmURL   string
	window   int
	pod      *httptest.Server
	image    string
	bad      map[string]bool
	starts   int
	removals int
}

func (r *podRuntime) start(image string) {
	r.closePod()
	if r.bad[image] {
		return // the image crashes on start: no pod ever becomes ready
	}
	a := agent.New(agent.Config{WorkspaceID: "a1", Version: image, WorkspaceDir: r.volume, CredentialDir: r.creds,
		LLMBaseURL: r.llmURL, Model: "m", ContextTokens: r.window, ReserveTokens: 200, KeepRecentTokens: 200})
	r.pod, r.image = httptest.NewServer(a.Handler()), image
	r.starts++
}

func (r *podRuntime) closePod() {
	if r.pod != nil {
		r.pod.Close()
	}
	r.pod, r.image = nil, ""
}

func (r *podRuntime) Ensure(_ context.Context, _ Workspace, p Profile) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pod == nil || r.image != p.Image {
		r.start(p.Image)
	}
	return nil
}

func (r *podRuntime) Observe(context.Context, Workspace, Profile) (Observation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pod == nil {
		return Observation{Exists: true}, nil
	}
	return Observation{Exists: true, Ready: true, Endpoint: r.pod.URL, Image: r.image}, nil
}

func (r *podRuntime) Stop(context.Context, Workspace) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closePod()
	return nil
}

func (r *podRuntime) Restart(_ context.Context, _ Workspace) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.start(r.image)
	return nil
}

func (r *podRuntime) Delete(context.Context, Workspace) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closePod()
	r.removals++
	return os.RemoveAll(r.volume)
}

func (r *podRuntime) Probe(context.Context) error { return nil }

func (r *podRuntime) podURL() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pod == nil {
		return ""
	}
	return r.pod.URL
}

type continuityEnv struct {
	t   *testing.T
	c   *Controller
	r   *podRuntime
	llm *fakellm.Server
	now time.Time
}

func newContinuityEnv(t *testing.T, window int) *continuityEnv {
	t.Helper()
	llm := fakellm.New("k1")
	llmSrv := httptest.NewServer(llm.Handler())
	t.Cleanup(llmSrv.Close)
	creds := t.TempDir()
	for name, content := range map[string]string{agent.CredentialKey: "k1", agent.VersionKey: "1"} {
		if err := os.WriteFile(filepath.Join(creds, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r := &podRuntime{volume: filepath.Join(t.TempDir(), "volume"), creds: creds, llmURL: llmSrv.URL + "/v1", window: window, bad: map[string]bool{}}
	t.Cleanup(func() { r.mu.Lock(); r.closePod(); r.mu.Unlock() })
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	profile := Profile{Image: imgV1, AllowedImages: []string{imgV2, "reg.example/agent:bad"}}
	c := New(s, r, map[string]Profile{"agent": profile}, time.Minute)
	e := &continuityEnv{t: t, c: c, r: r, llm: llm, now: time.Now()}
	c.now = func() time.Time { return e.now }
	c.started = e.now.Add(-48 * time.Hour)
	c.StartupGrace = 0
	c.DisableReadyCache = true
	c.PollInterval = time.Millisecond
	c.UpgradeSettle = 10 * time.Second
	if _, err := c.Create(testActor, "a1", "agent"); err != nil {
		t.Fatal(err)
	}
	return e
}

// wake does what the gateway does for a request: Acquire, which starts the
// workspace if it is not running, then talk to the endpoint it returns.
func (e *continuityEnv) wake() (endpoint string, release func()) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	endpoint, release, err := e.c.Acquire(ctx, "a1")
	if err != nil {
		e.t.Fatalf("wake: %v", err)
	}
	return endpoint, release
}

func chatAt(t *testing.T, endpoint, session, text string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"session": session, "message": text})
	resp, err := http.Post(endpoint+"/v1/chat", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Reply string `json:"reply"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("chat: %d %q %v", resp.StatusCode, out.Error, err)
	}
	return out.Reply
}

// converse starts the workspace on first use, plants a fact in session s, and
// returns once the pod has been used and released.
func (e *continuityEnv) plant(fact string) {
	e.t.Helper()
	endpoint, release := e.wake()
	defer release()
	if got := chatAt(e.t, endpoint, "s", "remember "+fact); !strings.HasPrefix(got, "echo(1)") {
		e.t.Fatalf("setup: %q", got)
	}
}

// expectRecall asks the model, in the same session, to recall the fact, after
// waking the workspace the way a request would. The fake answers "found" only
// if the fact is in the history the agent sent.
func (e *continuityEnv) expectRecall(fact string) {
	e.t.Helper()
	endpoint, release := e.wake()
	defer release()
	want := "recall(found): " + fact
	if got := chatAt(e.t, endpoint, "s", "recall "+fact); got != want {
		e.t.Fatalf("the session lost its context: got %q, want %q", got, want)
	}
	// A different session on the same volume must not see it.
	if got := chatAt(e.t, endpoint, "other", "recall "+fact); got != "recall(missing): "+fact {
		e.t.Fatalf("sessions leaked into each other: %q", got)
	}
}

func (e *continuityEnv) reconcile(n int) {
	e.t.Helper()
	for i := 0; i < n; i++ {
		if _, err := e.c.Reconcile(context.Background(), "a1"); err != nil {
			e.t.Fatal(err)
		}
		e.now = e.now.Add(time.Second)
	}
}

func (e *continuityEnv) expectPodGone(old string) {
	e.t.Helper()
	if e.r.podURL() != "" {
		e.t.Fatal("the pod is still running")
	}
	if _, err := http.Get(old + "/health"); err == nil {
		e.t.Fatal("the old pod still answers")
	}
}

func TestSessionSurvivesStopAndWakeOnRequest(t *testing.T) {
	e := newContinuityEnv(t, 0)
	e.plant("zx81")
	old := e.r.podURL()
	if _, err := e.c.SetDesired(testActor, "a1", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	e.reconcile(1)
	e.expectPodGone(old)
	starts := e.r.starts
	e.expectRecall("zx81")
	if e.r.starts != starts+1 {
		t.Fatalf("the request must have started a new pod: starts %d -> %d", starts, e.r.starts)
	}
	if e.r.removals != 0 {
		t.Fatal("the volume was removed")
	}
}

func TestSessionSurvivesIdleScaleToZero(t *testing.T) {
	e := newContinuityEnv(t, 0)
	e.plant("zx81")
	old := e.r.podURL()
	e.now = e.now.Add(5 * time.Minute) // well past the one-minute idle timeout
	e.reconcile(1)
	if w, _ := e.c.Get("a1"); w.Desired != DesiredStopped {
		t.Fatalf("setup: the idle reaper did not stop it: %+v", w)
	}
	e.expectPodGone(old)
	e.expectRecall("zx81")
}

func TestSessionSurvivesAnUpgradeThatIsCommitted(t *testing.T) {
	e := newContinuityEnv(t, 0)
	e.plant("zx81")
	old := e.r.podURL()
	if _, err := e.c.Upgrade(testActor, "a1", imgV2, 0); err != nil {
		t.Fatal(err)
	}
	e.reconcile(20)
	w, _ := e.c.Get("a1")
	if w.LastUpgrade == nil || w.LastUpgrade.Outcome != UpgradeCommitted || e.r.image != imgV2 {
		t.Fatalf("setup: the upgrade was not committed: %+v image=%s", w.LastUpgrade, e.r.image)
	}
	if e.r.podURL() == old {
		t.Fatal("the upgrade did not replace the pod")
	}
	e.expectRecall("zx81")
}

func TestSessionSurvivesAnUpgradeThatRollsBack(t *testing.T) {
	e := newContinuityEnv(t, 0)
	e.plant("zx81")
	e.r.bad["reg.example/agent:bad"] = true
	if _, err := e.c.Upgrade(testActor, "a1", "reg.example/agent:bad", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	e.reconcile(40)
	w, _ := e.c.Get("a1")
	if w.LastUpgrade == nil || w.LastUpgrade.Outcome != UpgradeRolledBack {
		t.Fatalf("setup: the upgrade was not rolled back: %+v", w.LastUpgrade)
	}
	e.reconcile(3)
	if e.r.image != imgV1 {
		t.Fatalf("setup: the old image is not serving again: %q", e.r.image)
	}
	e.expectRecall("zx81")
}

func TestCompactionSummarySurvivesStopAndWake(t *testing.T) {
	e := newContinuityEnv(t, 1000)
	endpoint, release := e.wake()
	for i := 0; i < 10; i++ {
		chatAt(t, endpoint, "s", fmt.Sprintf("turn%02d %s", i, strings.Repeat("alpha beta gamma ", 12)))
	}
	release()
	if summaries, _ := e.llm.Stats(); summaries == 0 {
		t.Fatal("setup: the session was never compacted")
	}
	raw, err := os.ReadFile(filepath.Join(e.r.volume, "summaries", "s.json"))
	var stored struct {
		Summary string `json:"summary"`
		Covers  int    `json:"covers"`
	}
	if err != nil || json.Unmarshal(raw, &stored) != nil || stored.Covers == 0 {
		t.Fatalf("setup: no summary on the volume: %s %v", raw, err)
	}
	// The fake summariser lists the first words of every user line it
	// summarised; the first of them belongs to a turn that is no longer in the
	// replayed history.
	_, list, _ := strings.Cut(stored.Summary, "users=[")
	list, _, _ = strings.Cut(list, "]")
	compacted, _, _ := strings.Cut(list, "|")
	if !strings.HasPrefix(compacted, "turn") {
		t.Fatalf("setup: cannot find a compacted turn in %q", stored.Summary)
	}
	old := e.r.podURL()
	if _, err := e.c.SetDesired(testActor, "a1", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	e.reconcile(1)
	e.expectPodGone(old)

	// That turn was compacted away before the stop, so the only place the new
	// pod can still find it is the stored summary.
	endpoint, release = e.wake()
	defer release()
	summariesBefore, _ := e.llm.Stats()
	if got := chatAt(t, endpoint, "s", "recall "+compacted); got != "recall(found): "+compacted {
		t.Fatalf("the summary did not survive the stop: %q", got)
	}
	if after, _ := e.llm.Stats(); after != summariesBefore {
		t.Fatalf("the new pod re-summarised instead of loading the stored summary: %d -> %d", summariesBefore, after)
	}
}

func TestSessionSurvivesLeaseSuspensionAndRenewal(t *testing.T) {
	e := newContinuityEnv(t, 0)
	e.plant("zx81")
	if _, err := e.c.SetExpiry(testActor, "a1", e.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	e.now = e.now.Add(time.Minute)
	e.reconcile(1) // the lease ran out: suspended, workload scaled to zero
	if w, _ := e.c.Get("a1"); w.Desired != DesiredSuspended {
		t.Fatalf("setup: %+v", w)
	}
	if _, err := os.Stat(filepath.Join(e.r.volume, "sessions", "s.jsonl")); err != nil || e.r.removals != 0 {
		t.Fatalf("suspension must keep the volume: %v removals=%d", err, e.r.removals)
	}
	if _, err := e.c.SetExpiry(testActor, "a1", e.now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	e.expectRecall("zx81") // renewed: wakes, and the session is still there
}

func heartbeatTotal(t *testing.T, endpoint string) int64 {
	t.Helper()
	resp, err := http.Get(endpoint + "/heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Usage Usage `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Usage.TotalTokens
}

func TestTokenCounterLivesOnTheVolumeNotInThePod(t *testing.T) {
	e := newContinuityEnv(t, 0)
	e.plant("zx81")
	endpoint, release := e.wake()
	before := heartbeatTotal(t, endpoint)
	release()
	if before == 0 {
		t.Fatal("setup: nothing was counted")
	}
	if _, err := e.c.SetDesired(testActor, "a1", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	e.reconcile(1)
	endpoint, release = e.wake()
	defer release()
	if after := heartbeatTotal(t, endpoint); after != before {
		t.Fatalf("a new pod restarted the counter: %d -> %d", before, after)
	}
}
