package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The chaos test drives the real Controller, Store and Scheduler against a
// simulated cluster that fails randomly, sometimes after applying the change
// (a lost response), while the controller itself is crashed and restarted at
// random points. Safety invariants are checked inside every destructive runtime
// call, against the durable store, so a violation is caught at the moment it
// would have happened in a real cluster.

type chaosWorkspace struct {
	deployment bool
	replicas   int
	pvc        bool
	everPVC    bool
	deleted    bool
}

type chaosCluster struct {
	mu         sync.Mutex
	rng        *rand.Rand
	failRate   float64
	items      map[string]*chaosWorkspace
	store      atomic.Pointer[Store]
	clock      *atomic.Int64
	grace      time.Duration
	open       func(id string) int64
	violations map[string]int
	faults     int
	calls      map[string]int
}

func (c *chaosCluster) now() time.Time { return time.Unix(0, c.clock.Load()) }

func (c *chaosCluster) item(id string) *chaosWorkspace {
	if c.items[id] == nil {
		c.items[id] = &chaosWorkspace{}
	}
	return c.items[id]
}

func (c *chaosCluster) violate(kind, id, detail string) {
	c.violations[kind]++
	if c.violations[kind] == 1 {
		fmt.Fprintf(os.Stderr, "chaos violation %s on %s: %s\n", kind, id, detail)
	}
}

// fault decides the outcome of one runtime call: succeed, fail before the
// change, or apply the change and still report failure.
func (c *chaosCluster) fault() (apply bool, err error) {
	if c.rng.Float64() >= c.failRate {
		return true, nil
	}
	c.faults++
	if c.rng.Intn(2) == 0 {
		return false, errors.New("injected failure before apply")
	}
	return true, errors.New("injected failure after apply")
}

func (c *chaosCluster) record(id string) (Workspace, bool) {
	w, err := c.store.Load().Get(id)
	return w, err == nil
}

func (c *chaosCluster) Ensure(_ context.Context, w Workspace, _ Profile) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls["ensure"]++
	it := c.item(w.ID)
	if it.deleted {
		c.violate("resurrected-after-delete", w.ID, "Ensure after hard delete")
	}
	if rec, ok := c.record(w.ID); ok && rec.Desired != DesiredRunning {
		c.violate("ensure-without-running-intent", w.ID, rec.Desired)
	}
	apply, err := c.fault()
	if apply {
		it.pvc, it.everPVC, it.deployment, it.replicas = true, true, true, 1
	}
	return err
}

func (c *chaosCluster) Observe(_ context.Context, w Workspace, _ Profile) (Observation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls["observe"]++
	if _, err := c.fault(); err != nil {
		return Observation{}, err
	}
	it := c.item(w.ID)
	ready := it.deployment && it.replicas == 1
	return Observation{Exists: it.deployment, Ready: ready, Endpoint: "http://agent.chaos"}, nil
}

func (c *chaosCluster) Stop(_ context.Context, w Workspace) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls["stop"]++
	it := c.item(w.ID)
	if c.open(w.ID) > 0 && it.replicas > 0 {
		c.violate("stopped-with-open-request", w.ID, "scale to zero under a live request")
	}
	apply, err := c.fault()
	if apply {
		it.replicas = 0
	}
	return err
}

func (c *chaosCluster) Delete(_ context.Context, w Workspace) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls["delete"]++
	now := c.now()
	rec, ok := c.record(w.ID)
	switch {
	case !ok || rec.Desired != DesiredDeleted:
		c.violate("delete-without-durable-intent", w.ID, rec.Desired)
	case rec.DeletionReason == DeletedByGrace && now.Sub(rec.SuspendedAt) < c.grace:
		c.violate("hard-delete-before-grace", w.ID, now.Sub(rec.SuspendedAt).String())
	}
	if ok && hasLease(rec, now) {
		c.violate("deleted-with-live-lease", w.ID, "")
	}
	if c.open(w.ID) > 0 {
		c.violate("deleted-with-open-request", w.ID, "")
	}
	apply, err := c.fault()
	if apply {
		it := c.item(w.ID)
		if it.pvc {
			c.calls["pvc-removed"]++
		}
		it.deployment, it.replicas, it.pvc, it.deleted = false, 0, false, true
	}
	return err
}

func (c *chaosCluster) Restart(_ context.Context, w Workspace) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls["restart"]++
	if !c.item(w.ID).deployment {
		return errors.New("deployment not found")
	}
	_, err := c.fault()
	return err
}

func (c *chaosCluster) Probe(context.Context) error { return nil }

// checkStorage runs between driver steps: a workspace which was never asked to
// be deleted must still have the volume it once had.
func (c *chaosCluster) checkStorage() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, it := range c.items {
		rec, ok := c.record(id)
		if ok && rec.Desired != DesiredDeleted && it.everPVC && !it.pvc {
			c.violate("pvc-lost", id, rec.Desired)
		}
	}
}

type chaosStats struct {
	Seed             int64          `json:"seed"`
	Steps            int            `json:"steps"`
	Crashes          int            `json:"crashes"`
	Faults           int            `json:"faults"`
	Rounds           int            `json:"rounds"`
	Requests         int            `json:"requests"`
	ConvergeRounds   int            `json:"converge_rounds"`
	Calls            map[string]int `json:"calls"`
	Violations       map[string]int `json:"violations"`
	FinalDesired     map[string]int `json:"final_desired"`
	ConcurrentRounds bool           `json:"concurrent_rounds"`
}

type openRequest struct {
	id      string
	release func()
}

func runChaos(t *testing.T, seed int64, steps int, concurrent bool) chaosStats {
	t.Helper()
	const workspaces = 4
	rng := rand.New(rand.NewSource(seed))
	clock := &atomic.Int64{}
	clock.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	openCount := map[string]*atomic.Int64{}
	ids := make([]string, workspaces)
	for i := range ids {
		ids[i] = "w" + strconv.Itoa(i)
		openCount[ids[i]] = &atomic.Int64{}
	}
	cluster := &chaosCluster{rng: rand.New(rand.NewSource(seed * 7919)), items: map[string]*chaosWorkspace{},
		clock: clock, grace: 10 * time.Minute, violations: map[string]int{}, calls: map[string]int{},
		open: func(id string) int64 { return openCount[id].Load() }}
	profiles := map[string]Profile{"demo": {}}
	dir := t.TempDir()
	stats := chaosStats{Seed: seed, Steps: steps, ConcurrentRounds: concurrent}

	var (
		c        *Controller
		store    *Store
		sched    *Scheduler
		bgCancel context.CancelFunc
		bgDone   chan struct{}
		requests []openRequest
		leases   = map[string][]string{}
	)
	start := func() {
		var err error
		store, err = OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		cluster.store.Store(store)
		c = New(store, cluster, profiles, time.Minute)
		c.now, c.started = now, now()
		c.GracePeriod, c.StartupGrace = cluster.grace, 30*time.Second
		c.OperationTimeout, c.PollInterval = time.Second, time.Millisecond
		sched = NewScheduler(c)
		sched.Concurrency, sched.BatchSize = 2, 2
		if concurrent {
			ctx, cancel := context.WithCancel(context.Background())
			bgCancel, bgDone = cancel, make(chan struct{})
			go func() {
				defer close(bgDone)
				for ctx.Err() == nil {
					sched.Round(ctx)
					time.Sleep(200 * time.Microsecond)
				}
			}()
		}
	}
	stopBackground := func() {
		if bgCancel != nil {
			bgCancel()
			<-bgDone
			bgCancel = nil
		}
	}
	crash := func() {
		stopBackground()
		// In-flight requests die with the process; nothing releases them.
		for _, r := range requests {
			openCount[r.id].Add(-1)
		}
		requests = nil
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		stats.Crashes++
		start()
	}
	setFail := func(rate float64) {
		cluster.mu.Lock()
		cluster.failRate = rate
		cluster.mu.Unlock()
	}

	start()
	for _, id := range ids {
		if _, err := c.Create("chaos", id, "demo"); err != nil {
			t.Fatal(err)
		}
	}
	setFail(0.2)
	for step := 0; step < steps; step++ {
		id := ids[rng.Intn(len(ids))]
		switch op := rng.Intn(100); {
		case op < 14:
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			_, release, err := c.Acquire(ctx, id)
			cancel()
			if err == nil {
				openCount[id].Add(1)
				requests = append(requests, openRequest{id, release})
				stats.Requests++
			}
		case op < 26:
			if len(requests) > 0 {
				i := rng.Intn(len(requests))
				r := requests[i]
				requests = append(requests[:i], requests[i+1:]...)
				openCount[r.id].Add(-1)
				r.release()
			}
		case op < 34:
			_, _ = c.SetDesired("chaos", id, DesiredRunning)
		case op < 42:
			_, _ = c.SetDesired("chaos", id, DesiredStopped)
		case op < 45:
			_, _ = c.SetDesired("chaos", id, DesiredDeleted)
		case op < 50:
			_, _ = c.Restart("chaos", id)
		case op < 56:
			var at time.Time
			switch rng.Intn(3) {
			case 0:
				at = now().Add(-time.Second)
			case 1:
				at = now().Add(time.Duration(rng.Intn(30)+1) * time.Minute)
			}
			_, _ = c.SetExpiry("chaos", id, at)
		case op < 62:
			if token, err := c.Lease("chaos", id, "", time.Duration(rng.Intn(20)+1)*time.Minute); err == nil {
				leases[id] = append(leases[id], token)
			}
		case op < 66:
			if tokens := leases[id]; len(tokens) > 0 {
				i := rng.Intn(len(tokens))
				_ = c.ReleaseLease("chaos", id, tokens[i])
				leases[id] = append(tokens[:i], tokens[i+1:]...)
			}
		case op < 78:
			clock.Add(int64(time.Duration(rng.Int63n(int64(3 * time.Minute)))))
		case op < 80:
			clock.Add(int64(cluster.grace + time.Duration(rng.Intn(120))*time.Second))
		case op < 92:
			sched.Round(context.Background())
			stats.Rounds++
		case op < 96:
			crash()
		default:
			setFail([]float64{0, 0.2, 0.5}[rng.Intn(3)])
		}
		cluster.checkStorage()
	}

	// Heal: no more faults or traffic. Every workspace must converge to what
	// its durable intent says, within a bounded number of rounds.
	stopBackground()
	for _, r := range requests {
		openCount[r.id].Add(-1)
		r.release()
	}
	requests = nil
	setFail(0)
	converged := false
	for round := 1; round <= 10 && !converged; round++ {
		clock.Add(int64(time.Second))
		sched.Round(context.Background())
		stats.ConvergeRounds = round
		converged = true
		cluster.mu.Lock()
		for _, id := range ids {
			w, err := store.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			it := cluster.item(id)
			var ok bool
			switch w.Desired {
			case DesiredRunning:
				ok = it.deployment && it.replicas == 1 && w.Phase == PhaseRunning
			case DesiredStopped, DesiredSuspended:
				ok = it.replicas == 0 && (!it.everPVC || it.pvc) && w.Phase != PhaseError
			case DesiredDeleted:
				ok = !it.deployment && !it.pvc && w.Phase == PhaseDeleted
			}
			converged = converged && ok
		}
		cluster.mu.Unlock()
	}
	if !converged {
		cluster.violations["no-convergence"]++
	}
	cluster.checkStorage()
	stats.FinalDesired = map[string]int{}
	for _, id := range ids {
		w, _ := store.Get(id)
		stats.FinalDesired[w.Desired]++
	}
	_ = store.Close()
	cluster.mu.Lock()
	stats.Faults, stats.Calls, stats.Violations = cluster.faults, cluster.calls, cluster.violations
	cluster.mu.Unlock()
	return stats
}

func chaosEnvInt(name string, fallback int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n > 0 {
		return n
	}
	return fallback
}

// TestChaosLifecycleInvariants is a small default run; set
// AGENT_WORKSPACE_CHAOS_SEEDS / _STEPS for a long campaign and
// AGENT_WORKSPACE_CHAOS_OUT to keep per-seed statistics.
func TestChaosLifecycleInvariants(t *testing.T) {
	seeds := chaosEnvInt("AGENT_WORKSPACE_CHAOS_SEEDS", 6)
	steps := chaosEnvInt("AGENT_WORKSPACE_CHAOS_STEPS", 120)
	total := chaosStats{Calls: map[string]int{}, Violations: map[string]int{}}
	maxConverge := 0
	var all []chaosStats
	for _, concurrent := range []bool{false, true} {
		for seed := int64(1); seed <= int64(seeds); seed++ {
			s := runChaos(t, seed, steps, concurrent)
			all = append(all, s)
			total.Steps += s.Steps
			total.Crashes += s.Crashes
			total.Faults += s.Faults
			total.Rounds += s.Rounds
			total.Requests += s.Requests
			for k, v := range s.Calls {
				total.Calls[k] += v
			}
			for k, v := range s.Violations {
				total.Violations[k] += v
			}
			if s.ConvergeRounds > maxConverge {
				maxConverge = s.ConvergeRounds
			}
			if len(s.Violations) > 0 {
				t.Errorf("seed %d concurrent=%v: violations %v", seed, concurrent, s.Violations)
			}
		}
	}
	keys := make([]string, 0, len(total.Calls))
	for k := range total.Calls {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("runs=%d steps=%d crashes=%d injected_faults=%d requests=%d max_converge_rounds=%d violations=%v",
		len(all), total.Steps, total.Crashes, total.Faults, total.Requests, maxConverge, total.Violations)
	for _, k := range keys {
		t.Logf("runtime %s calls: %d", k, total.Calls[k])
	}
	// Guard against a vacuous run: the campaign must have exercised crashes,
	// failures and every destructive path.
	if total.Crashes == 0 || total.Faults == 0 || total.Calls["stop"] == 0 || total.Calls["pvc-removed"] == 0 {
		t.Fatalf("campaign did not exercise the destructive paths: %+v", total)
	}
	if out := os.Getenv("AGENT_WORKSPACE_CHAOS_OUT"); out != "" {
		b, _ := json.MarshalIndent(map[string]any{"total": total, "max_converge_rounds": maxConverge, "runs": all}, "", "  ")
		if err := os.WriteFile(out, b, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
