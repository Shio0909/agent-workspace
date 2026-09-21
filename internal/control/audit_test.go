package control

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func actionsOf(events []AuditEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.Action)
	}
	return out
}

func TestAuditRecordsEveryLifecycleActionAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := &fakeRuntime{}
	profiles := map[string]Profile{"demo": {}}
	c := New(s, r, profiles, time.Minute)
	if _, err := c.Create("alice", "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	for _, step := range []func() error{
		func() error { _, err := c.SetDesired("alice", "demo", DesiredRunning); return err },
		func() error { _, err := c.Lease("alice", "demo", "", time.Minute); return err },
		func() error { return c.ReleaseLease("alice", "demo", leaseOf(t, c)) },
		func() error { _, err := c.Restart("alice", "demo"); return err },
		func() error { _, err := c.SetExpiry("alice", "demo", time.Now().Add(time.Hour)); return err },
		func() error { _, err := c.SetDesired("alice", "demo", DesiredStopped); return err },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	all, err := c.Audit(AuditQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 7 {
		t.Fatalf("expected an event per action, got %d: %v", len(all), actionsOf(all))
	}
	for _, want := range []string{ActionCreate, ActionStart, ActionLeaseAcquire, ActionLeaseRelease, ActionRestart, ActionExpirySet, ActionStop} {
		if !contains(actionsOf(all), want) {
			t.Fatalf("missing audit action %q in %v", want, actionsOf(all))
		}
	}
	if all[0].Actor != "alice" || all[0].Workspace != "demo" || all[0].Result != ResultOK || all[0].At.IsZero() {
		t.Fatalf("event is missing attribution: %+v", all[0])
	}
	// 重启进程：换一个 Store 实例重新打开同一个数据目录。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after, err := New(s, r, profiles, time.Minute).Audit(AuditQuery{Workspace: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(all) {
		t.Fatalf("audit did not survive the restart: %d -> %d", len(all), len(after))
	}
	if after[0].At != all[0].At || after[0].Action != all[0].Action {
		t.Fatal("restart rewrote the audit trail")
	}
	// 审计是磁盘上的 JSONL：离开本进程也读得懂。
	raw, err := os.ReadFile(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(strings.TrimSpace(string(raw)), "\n") + 1; lines != len(all) {
		t.Fatalf("expected %d jsonl lines, file has %d", len(all), lines)
	}
}

// leaseOf 取当前唯一租约的 token，用于释放。
func leaseOf(t *testing.T, c *Controller) string {
	t.Helper()
	w, err := c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	for token := range w.Leases {
		return token
	}
	t.Fatal("no lease to release")
	return ""
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func TestAuditQueryFiltersAndLimits(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := New(s, &fakeRuntime{}, map[string]Profile{"demo": {}}, time.Minute)
	now := time.Now()
	c.now = func() time.Time { return now }
	for _, id := range []string{"demo", "other"} {
		if _, err := c.Create("alice", id, "demo"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.SetDesired("bob", "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if _, err := c.SetDesired("bob", "other", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		query AuditQuery
		want  int
	}{
		"all":            {AuditQuery{}, 4},
		"by workspace":   {AuditQuery{Workspace: "demo"}, 2},
		"by action":      {AuditQuery{Action: ActionStart}, 2},
		"by since":       {AuditQuery{Since: now}, 1},
		"by until":       {AuditQuery{Until: now}, 3},
		"combined":       {AuditQuery{Workspace: "demo", Action: ActionStart}, 1},
		"limit":          {AuditQuery{Limit: 2}, 2},
		"no match":       {AuditQuery{Workspace: "ghost"}, 0},
		"empty range":    {AuditQuery{Since: now, Until: now}, 0},
		"actor unlisted": {AuditQuery{Action: ActionHardDelete}, 0},
	} {
		got, err := c.Audit(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != tc.want {
			t.Fatalf("%s: got %d events, want %d (%v)", name, len(got), tc.want, actionsOf(got))
		}
	}
}

func TestAuditAppendOnlyAndTornLineTolerant(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenAuditLog(filepath.Join(dir, "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	first := AuditEvent{At: time.Unix(1, 0).UTC(), Actor: "alice", Action: ActionCreate, Workspace: "demo", Result: ResultOK}
	if err := log.Append(first); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(log.Path())
	if err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃时写了一半的行，并重新打开：那半行应该被跳过，而不是把后面
	// 追加的记录一起写坏。
	torn, err := os.OpenFile(log.Path(), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := torn.WriteString(`{"at":"2026-01-01T00:00:00Z","actor":"ali`); err != nil {
		t.Fatal(err)
	}
	torn.Close()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if log, err = OpenAuditLog(log.Path()); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(AuditEvent{At: time.Unix(2, 0).UTC(), Actor: "bob", Action: ActionStop, Workspace: "demo", Result: ResultOK}); err != nil {
		t.Fatal(err)
	}
	events, err := log.Query(AuditQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Action != ActionCreate || events[1].Action != ActionStop {
		t.Fatalf("torn line changed the result: %+v", events)
	}
	// 追加不会重写历史：已经落盘的前缀必须逐字节不变。
	after, err := os.ReadFile(log.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(after), string(before)) {
		t.Fatal("audit log was rewritten instead of appended")
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	// 文件不存在时查询返回空而不是报错：全新数据目录是正常状态。
	empty, err := OpenAuditLog(filepath.Join(dir, "missing.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if events, err := empty.Query(AuditQuery{}); err != nil || len(events) != 0 {
		t.Fatalf("empty log: %v %d", err, len(events))
	}
}

func TestAuditRecordsFailedReclamation(t *testing.T) {
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.StartupGrace, c.GracePeriod = 0, time.Minute
	r.failStop = true
	if _, err := c.SetExpiry(testActor, "demo", now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err == nil {
		t.Fatal("expected the suspension to fail")
	}
	events, err := c.Audit(AuditQuery{Action: ActionSuspend})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Result != ResultError || !strings.Contains(events[0].Detail, "temporary Kubernetes failure") {
		t.Fatalf("failed suspension was not audited: %+v", events)
	}
	// 每次重试都失败时不再重复记录：坏掉的后端不该把审计日志刷爆。
	for i := 0; i < 3; i++ {
		_, _ = c.Reconcile(context.Background(), "demo")
	}
	events, err = c.Audit(AuditQuery{Action: ActionSuspend})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("repeated failures flooded the audit log: %d", len(events))
	}
}

func TestAuditFailureIsCountedNotFatal(t *testing.T) {
	c, _ := fixture(t)
	// 让审计文件不可写：动作本身仍然必须成功，失败只体现在计数和日志里。
	if err := c.store.Audit().Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatalf("a broken audit log must not fail the action: %v", err)
	}
	if c.Metrics.AuditFailures.Load() != 1 {
		t.Fatalf("audit failure was not counted: %d", c.Metrics.AuditFailures.Load())
	}
	w, err := c.Get("demo")
	if err != nil || w.Desired != DesiredRunning {
		t.Fatalf("state was rolled back by an audit failure: %+v %v", w, err)
	}
}

func TestAuditRecordsIdleStop(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started, c.IdleTimeout = now.Add(-2*time.Hour), time.Hour
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	events, err := c.Audit(AuditQuery{Action: ActionIdleStop})
	if err != nil || len(events) != 1 {
		t.Fatalf("idle stop was not audited: %v %+v", err, events)
	}
	if events[0].Actor != ActorSystem || !strings.Contains(events[0].Detail, "volume retained") {
		t.Fatalf("idle stop lost its attribution: %+v", events[0])
	}
	if w, _ := c.Get("demo"); w.Desired != DesiredStopped {
		t.Fatalf("idle stop did not take effect: %+v", w)
	}
}
