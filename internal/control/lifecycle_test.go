package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// lifecycleFixture 把空闲缩容关掉，让"什么时候进入 suspended"只由租期决定，
// 断言不会因为同时触发了空闲回收而变得含糊。
func lifecycleFixture(t *testing.T) (*Controller, *fakeRuntime) {
	t.Helper()
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started = now
	c.IdleTimeout, c.StartupGrace, c.GracePeriod = time.Hour, 0, time.Hour
	return c, r
}

func TestSuspendedKeepsStorageUntilGraceExpires(t *testing.T) {
	c, r := lifecycleFixture(t)
	ctx := context.Background()
	now := c.now()
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// 到期：running -> suspended，工作负载停掉，存储必须留下。
	now = now.Add(2 * time.Minute)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	w, err := c.Get("demo")
	if err != nil {
		t.Fatalf("suspended workspace disappeared: %v", err)
	}
	if w.Desired != DesiredSuspended || w.Phase != PhaseSuspended {
		t.Fatalf("workspace was not suspended: %+v", w)
	}
	if w.SuspendedAt.IsZero() || !w.ExpiresAt.Equal(now.Add(-time.Minute)) {
		t.Fatalf("suspension did not record why: %+v", w)
	}
	if _, _, deletes, _ := r.counts(); deletes != 0 {
		t.Fatal("suspension deleted storage")
	}
	if _, stops, _, _ := r.counts(); stops == 0 {
		t.Fatal("suspension left the workload running")
	}
	// 挂起态不接受使用，也不能靠 start 绕过：必须先续期。
	if _, _, err := c.Acquire(ctx, "demo"); !errors.Is(err, ErrExpired) {
		t.Fatalf("suspended workspace served traffic: %v", err)
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); !errors.Is(err, ErrExpired) {
		t.Fatalf("suspended workspace accepted start: %v", err)
	}
	if _, err := c.Lease(testActor, "demo", "", time.Minute); !errors.Is(err, ErrExpired) {
		t.Fatalf("suspended workspace accepted a lease: %v", err)
	}
	// 模拟控制器重启：宽限期计时必须延续，而不是重新开始。
	restarted := New(c.store, r, c.profiles, time.Hour)
	restarted.now, restarted.started = c.now, now
	restarted.IdleTimeout, restarted.StartupGrace, restarted.GracePeriod = time.Hour, 0, time.Hour
	if _, err := restarted.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, deletes, _ := r.counts(); deletes != 0 {
		t.Fatal("restart deleted a suspended workspace early")
	}
	// 宽限期满：这才是唯一会删除存储的一跳。
	now = now.Add(time.Hour)
	restarted.now = func() time.Time { return now }
	if _, err := restarted.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, deletes, _ := r.counts(); deletes != 1 {
		t.Fatalf("grace period did not lead to a hard delete: deletes=%d", deletes)
	}
	w, err = restarted.Get("demo")
	if err != nil || w.Desired != DesiredDeleted || w.Phase != PhaseDeleted {
		t.Fatalf("hard delete left the wrong state: %+v %v", w, err)
	}
	// 墓碑不会被再次回收。
	if _, err := restarted.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, deletes, _ := r.counts(); deletes != 1 {
		t.Fatalf("reconciled a tombstone: deletes=%d", deletes)
	}
	events, err := restarted.Audit(AuditQuery{Workspace: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	var chain []string
	for _, e := range events {
		switch e.Action {
		case ActionStart, ActionExpire, ActionSuspend, ActionHardDelete:
			chain = append(chain, e.Action)
		}
	}
	want := []string{ActionStart, ActionExpire, ActionSuspend, ActionHardDelete}
	if len(chain) != len(want) {
		t.Fatalf("audit chain is %v, want %v", chain, want)
	}
	for i := range want {
		if chain[i] != want[i] {
			t.Fatalf("audit chain is %v, want %v", chain, want)
		}
	}
}

func TestExpiredWorkspaceMustBeRenewedBeforeUse(t *testing.T) {
	c, _ := lifecycleFixture(t)
	ctx := context.Background()
	now := c.now()
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	c.now = func() time.Time { return now }
	// 还没轮到扫描，工作区在快照里仍是 running；调用方不能靠这个空档拿到服务。
	if _, _, err := c.Acquire(ctx, "demo"); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired workspace was woken up: %v", err)
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired workspace accepted start: %v", err)
	}
	if _, err := c.Restart(testActor, "demo"); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired workspace accepted restart: %v", err)
	}
	if _, err := c.Lease(testActor, "demo", "", time.Minute); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired workspace accepted a lease: %v", err)
	}
	// 续期之后一切恢复正常。
	if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Acquire(ctx, "demo"); err != nil {
		t.Fatalf("renewed workspace was not usable: %v", err)
	}
}

func TestSetExpiryResumesSuspendedWorkspace(t *testing.T) {
	c, r := lifecycleFixture(t)
	ctx := context.Background()
	now := c.now()
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	// 人工恢复：只改意图，不碰存储。
	if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	w, err := c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w.Desired != DesiredStopped || !w.SuspendedAt.IsZero() {
		t.Fatalf("suspended workspace was not resumed: %+v", w)
	}
	if _, _, deletes, _ := r.counts(); deletes != 0 {
		t.Fatal("resume touched storage")
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatalf("resumed workspace rejected start: %v", err)
	}
	events, err := c.Audit(AuditQuery{Action: ActionResume})
	if err != nil || len(events) != 1 {
		t.Fatalf("resume was not audited: %v %+v", err, events)
	}
	// 清除租期同样可以恢复，并且之后不会再被回收。
	if _, err := c.SetExpiry(testActor, "demo", time.Time{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(100 * time.Hour)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if updated, _ := c.Get("demo"); updated.Desired == DesiredSuspended {
		t.Fatal("workspace without a deadline was suspended")
	}
	if _, _, deletes, _ := r.counts(); deletes != 0 {
		t.Fatal("workspace without a deadline was deleted")
	}
}

func TestStartupGraceDefersReclamation(t *testing.T) {
	c, r := lifecycleFixture(t)
	ctx := context.Background()
	now := c.now()
	if _, err := c.SetExpiry(testActor, "demo", now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	w, err := c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	w.SuspendedAt = now.Add(-2 * time.Hour) // 宽限期也已经过完，只剩启动静默期挡着
	if err := c.store.Put(w); err != nil {
		t.Fatal(err)
	}
	c.StartupGrace = 5 * time.Minute
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, deletes, _ := r.counts(); deletes != 0 {
		t.Fatal("reclaimed during the startup grace period")
	}
	if got, _ := c.Get("demo"); got.Desired != DesiredStopped {
		t.Fatalf("startup grace did not preserve the workspace: %+v", got)
	}
	// 静默期结束后，同样的状态继续被回收：先挂起，宽限期满再硬删。
	now = now.Add(6 * time.Minute)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Get("demo"); got.Desired != DesiredSuspended {
		t.Fatalf("reclamation did not resume after the startup grace: %+v", got)
	}
	now = now.Add(2 * time.Hour)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, deletes, _ := r.counts(); deletes != 1 {
		t.Fatalf("grace period did not lead to a hard delete: deletes=%d", deletes)
	}
}

func TestDeadlineDoesNotCutOffLeasedWork(t *testing.T) {
	c, r := lifecycleFixture(t)
	ctx := context.Background()
	now := c.now()
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Lease(testActor, "demo", "", 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, stops, deletes, _ := r.counts(); stops != 0 || deletes != 0 {
		t.Fatal("a leased background task was cut off by the deadline")
	}
	if w, _ := c.Get("demo"); w.Desired != DesiredRunning {
		t.Fatalf("leased workspace left the running state: %+v", w)
	}
	// 租约一过期，回收照常推进。
	now = now.Add(20 * time.Minute)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if w, _ := c.Get("demo"); w.Desired != DesiredSuspended {
		t.Fatalf("expired workspace was not suspended after the lease ended: %+v", w)
	}
}

func TestRestartIntentIsDurableAndSparesStorage(t *testing.T) {
	c, r := lifecycleFixture(t)
	ctx := context.Background()
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Restart(testActor, "demo"); err != nil {
		t.Fatal(err)
	}
	if w, _ := c.Get("demo"); !w.RestartPending || w.Phase != PhaseStarting {
		t.Fatalf("restart intent was not recorded: %+v", w)
	}
	r.failRestart = true
	if _, err := c.Reconcile(ctx, "demo"); err == nil {
		t.Fatal("expected the restart to fail")
	}
	if w, _ := c.Get("demo"); !w.RestartPending {
		t.Fatal("a failed restart dropped the intent instead of retrying it")
	}
	r.failRestart = false
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	_, stops, deletes, restarts := r.counts()
	if restarts != 2 || stops != 0 || deletes != 0 {
		t.Fatalf("restart disturbed the lifecycle: restarts=%d stops=%d deletes=%d", restarts, stops, deletes)
	}
	if w, _ := c.Get("demo"); w.RestartPending {
		t.Fatal("restart intent survived a successful restart")
	}
	// 有活跃请求时重启必须被拒绝：它会把正在进行的响应截断。
	if _, release, err := c.Acquire(ctx, "demo"); err != nil {
		t.Fatal(err)
	} else {
		defer release()
	}
	if _, err := c.Restart(testActor, "demo"); !errors.Is(err, ErrConflict) {
		t.Fatalf("restart ignored an active request: %v", err)
	}
}

func TestExplicitDeleteSkipsTheGracePeriod(t *testing.T) {
	c, r := lifecycleFixture(t)
	ctx := context.Background()
	now := c.now()
	if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	// 用户显式删除：不必等宽限期。
	if _, err := c.SetDesired(testActor, "demo", DesiredDeleted); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, deletes, _ := r.counts(); deletes != 1 {
		t.Fatalf("explicit delete did not remove storage: deletes=%d", deletes)
	}
	events, err := c.Audit(AuditQuery{Workspace: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	var trigger string
	for _, e := range events {
		if e.Action == ActionHardDelete {
			trigger = e.Detail
		}
	}
	if trigger != "user-requested, deployment, service and pvc removed" {
		t.Fatalf("hard delete did not record who asked for it: %q", trigger)
	}
}

func TestGraceTriggeredDeleteKeepsItsReasonAcrossRetries(t *testing.T) {
	c, r := lifecycleFixture(t)
	ctx := context.Background()
	now := c.now()
	if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	c.now = func() time.Time { return now }
	r.failDelete = true
	if _, err := c.Reconcile(ctx, "demo"); err == nil {
		t.Fatal("expected the hard delete to fail")
	}
	r.failDelete = false
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	events, err := c.Audit(AuditQuery{Action: ActionHardDelete})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected a failed and a successful hard delete, got %+v", events)
	}
	if events[0].Result != ResultError {
		t.Fatalf("the failure was not recorded: %+v", events[0])
	}
	// 触发者来自持久化状态：宽限期这次转移发生在两轮之前，仍然要说得清。
	if events[1].Result != ResultOK || !strings.HasPrefix(events[1].Detail, "grace period elapsed") {
		t.Fatalf("the retry lost the trigger: %+v", events[1])
	}
	if w, _ := c.Get("demo"); w.DeletionReason != DeletedByGrace {
		t.Fatalf("deletion reason was not persisted: %+v", w)
	}
}
