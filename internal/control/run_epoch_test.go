package control

import (
	"testing"
	"time"
)

func TestPutAdvancesRunEpochOnlyWhenWorkloadIsReplaced(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Workspace)
		bumps  bool
	}{
		{"stop", func(w *Workspace) { w.Desired = DesiredStopped }, true},
		{"suspend", func(w *Workspace) { w.Desired = DesiredSuspended }, true},
		{"restart requested", func(w *Workspace) { w.RestartPending = true }, true},
		{"rollout requested", func(w *Workspace) { w.RolloutPending = true }, true},
		{"image changed", func(w *Workspace) { w.Image = "registry.example/agent:v2" }, true},
		{"heartbeat applied", func(w *Workspace) {
			w.HeartbeatAt, w.AgentVersion, w.Unresponsive = time.Now(), "v9", true
		}, false},
		{"usage and activity flushed", func(w *Workspace) {
			w.LastActivity, w.LastError, w.Usage.TotalTokens = time.Now(), "x", 42
		}, false},
		{"phase moved", func(w *Workspace) { w.Phase = PhaseRunning }, false},
		{"nothing", func(*Workspace) {}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := fixture(t)
			if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
				t.Fatal(err)
			}
			before, err := c.store.Get("demo")
			if err != nil {
				t.Fatal(err)
			}
			next := before
			tc.mutate(&next)
			if err := c.store.Put(next); err != nil {
				t.Fatal(err)
			}
			after, err := c.store.Get("demo")
			if err != nil {
				t.Fatal(err)
			}
			want := before.RunEpoch
			if tc.bumps {
				want++
			}
			if after.RunEpoch != want {
				t.Fatalf("run epoch = %d, want %d", after.RunEpoch, want)
			}
		})
	}
}

func TestRestartIntentBeingAppliedAlsoStartsANewRun(t *testing.T) {
	c, _ := fixture(t)
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Restart(testActor, "demo"); err != nil {
		t.Fatal(err)
	}
	requested, _ := c.store.Get("demo")
	if _, err := c.Reconcile(t.Context(), "demo"); err != nil {
		t.Fatal(err)
	}
	applied, _ := c.store.Get("demo")
	if applied.RestartPending {
		t.Fatal("the restart was not applied")
	}
	// 记录意图和执行意图各算一次：pod 是在后者被换掉的，在两者之间发出的
	// 心跳打到的仍是旧 pod。
	if applied.RunEpoch <= requested.RunEpoch {
		t.Fatalf("applying the restart did not start a new run: %d -> %d", requested.RunEpoch, applied.RunEpoch)
	}
}

func TestRunEpochIsStoreManaged(t *testing.T) {
	c, _ := fixture(t)
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	w, _ := c.store.Get("demo")
	w.RunEpoch = 999 // 调用方手里的旧副本或伪造值都不能改写代次
	if err := c.store.Put(w); err != nil {
		t.Fatal(err)
	}
	got, _ := c.store.Get("demo")
	if got.RunEpoch == 999 {
		t.Fatal("a caller-supplied run epoch was stored")
	}
}

func TestRunEpochSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := New(s, &fakeRuntime{}, map[string]Profile{"demo": {}}, time.Minute)
	if _, err := c.Create(testActor, "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{DesiredRunning, DesiredStopped, DesiredRunning} {
		if _, err := c.SetDesired(testActor, "demo", d); err != nil {
			t.Fatal(err)
		}
	}
	want, _ := s.Get("demo")
	if want.RunEpoch < 3 {
		t.Fatalf("epoch did not advance: %d", want.RunEpoch)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if got.RunEpoch != want.RunEpoch {
		t.Fatalf("run epoch not persisted: %d, want %d", got.RunEpoch, want.RunEpoch)
	}
}
