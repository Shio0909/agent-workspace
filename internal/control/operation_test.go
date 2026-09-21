package control

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// submit 模拟一次完整的 HTTP 写请求：占用幂等记录、执行动作、收尾。
// 返回的 proceeded 表示这次提交是否真的执行了动作。
func submit(t *testing.T, c *Controller, bizID, id, action string) (proceeded bool) {
	t.Helper()
	if _, err := c.BeginOperation(bizID, id, action); err != nil {
		return false
	}
	var applyErr error
	switch action {
	case OpStart:
		_, applyErr = c.SetDesired(testActor, id, DesiredRunning)
	case OpStop:
		_, applyErr = c.SetDesired(testActor, id, DesiredStopped)
	case OpRestart:
		_, applyErr = c.Restart(testActor, id)
	case OpDelete:
		_, applyErr = c.SetDesired(testActor, id, DesiredDeleted)
	default:
		t.Fatalf("unknown action %q", action)
	}
	if err := c.FinishOperation(bizID, applyErr); err != nil {
		t.Fatalf("finish operation: %v", err)
	}
	return true
}

func TestConcurrentSubmissionsOfOneBizIDApplyOnce(t *testing.T) {
	c, r := fixture(t)
	const submissions = 16
	var proceeded atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < submissions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.BeginOperation("biz-concurrent", "demo", OpDelete)
			if err != nil {
				if !errors.Is(err, ErrOperationInProgress) && !errors.Is(err, ErrOperationSucceeded) {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			proceeded.Add(1)
			_, applyErr := c.SetDesired(testActor, "demo", DesiredDeleted)
			if applyErr != nil {
				t.Errorf("apply: %v", applyErr)
			}
			if err := c.FinishOperation("biz-concurrent", applyErr); err != nil {
				t.Errorf("finish: %v", err)
			}
		}()
	}
	wg.Wait()
	if proceeded.Load() != 1 {
		t.Fatalf("wanted exactly one executor, got %d", proceeded.Load())
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, deletes, _ := r.counts(); deletes != 1 {
		t.Fatalf("duplicate submissions reached the runtime: deletes=%d", deletes)
	}
}

func TestReplayedOperationDoesNotRepeatItsSideEffect(t *testing.T) {
	c, r := fixture(t)
	ctx := context.Background()
	if !submit(t, c, "biz-restart", "demo", OpRestart) {
		t.Fatal("first submission did not proceed")
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, restarts := r.counts(); restarts != 1 {
		t.Fatalf("wanted one restart, got %d", restarts)
	}
	// 重放同一个 biz_id：结论是"已经成功"，既不再改状态，也不再有副作用。
	op, err := c.BeginOperation("biz-restart", "demo", OpRestart)
	if !errors.Is(err, ErrOperationSucceeded) {
		t.Fatalf("replay did not report success: %v", err)
	}
	if op.Status != OpSuccess || op.FinishedAt.IsZero() {
		t.Fatalf("replayed record is not finished: %+v", op)
	}
	w, err := c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w.RestartPending {
		t.Fatal("replay reintroduced the restart intent")
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, restarts := r.counts(); restarts != 1 {
		t.Fatalf("replay caused a second restart: %d", restarts)
	}
	// 换一个新的 biz_id 则是一次新的请求，副作用照样发生。
	if !submit(t, c, "biz-restart-2", "demo", OpRestart) {
		t.Fatal("a fresh biz_id was treated as a replay")
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, restarts := r.counts(); restarts != 2 {
		t.Fatalf("fresh biz_id did not apply: restarts=%d", restarts)
	}
}

func TestFailedOperationStaysFailedOnReplay(t *testing.T) {
	c, _ := fixture(t)
	if _, err := c.BeginOperation("biz-fail", "missing", OpStart); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "missing", DesiredRunning); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the workspace to be missing: %v", err)
	} else if err := c.FinishOperation("biz-fail", err); err != nil {
		t.Fatal(err)
	}
	op, err := c.BeginOperation("biz-fail", "missing", OpStart)
	if !errors.Is(err, ErrOperationFailed) {
		t.Fatalf("failed operation was not replayed as failed: %v", err)
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("original error text was lost: %v", err)
	}
	if op.Status != OpFailed || op.Error == "" {
		t.Fatalf("record does not describe the failure: %+v", op)
	}
}

func TestOperationKeyCoversTypeAndWorkspace(t *testing.T) {
	c, _ := fixture(t)
	if !submit(t, c, "biz-typed", "demo", OpStart) {
		t.Fatal("first submission did not proceed")
	}
	for _, tc := range []struct{ workspace, action string }{
		{"demo", OpStop},
		{"other", OpStart},
	} {
		_, err := c.BeginOperation("biz-typed", tc.workspace, tc.action)
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("%s/%s was accepted for a used biz_id: %v", tc.workspace, tc.action, err)
		}
	}
	op, err := c.store.GetOperation("biz-typed")
	if err != nil {
		t.Fatal(err)
	}
	if op.Type != OpStart || op.Workspace != "demo" {
		t.Fatalf("stored record was overwritten: %+v", op)
	}
}

func TestStaleProcessingRecordIsTakenOver(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.OperationLease = time.Minute
	if _, err := c.BeginOperation("biz-stale", "demo", OpStart); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	if _, err := c.BeginOperation("biz-stale", "demo", OpStart); !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("a live operation was taken over: %v", err)
	}
	// 控制器在动作中途崩溃会留下永不结束的 processing 记录，超过存活上限后
	// 必须可以被接管，否则这个 biz_id 就永久锁死了。
	now = now.Add(time.Minute)
	taken, err := c.BeginOperation("biz-stale", "demo", OpStart)
	if err != nil {
		t.Fatalf("stale record was not taken over: %v", err)
	}
	if !taken.StartedAt.Equal(now) || taken.Status != OpProcessing {
		t.Fatalf("takeover did not refresh the record: %+v", taken)
	}
	if c.Metrics.OperationTakeovers.Load() != 1 {
		t.Fatal("takeover was not counted")
	}
}

func TestFinishOperationIsFirstWriteWins(t *testing.T) {
	c, _ := fixture(t)
	if _, err := c.BeginOperation("biz-once", "demo", OpStop); err != nil {
		t.Fatal(err)
	}
	if err := c.FinishOperation("biz-once", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.FinishOperation("biz-once", errors.New("late duplicate")); err != nil {
		t.Fatal(err)
	}
	op, err := c.store.GetOperation("biz-once")
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != OpSuccess || op.Error != "" {
		t.Fatalf("a duplicate finish overwrote the terminal state: %+v", op)
	}
	if err := c.FinishOperation("biz-unknown", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("finishing an unknown record was accepted: %v", err)
	}
}

func TestOperationRetentionEvictsOnlyFinishedRecords(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxOperations = 2
	c := New(s, &fakeRuntime{}, map[string]Profile{"demo": {}}, time.Minute)
	now := time.Now()
	c.now = func() time.Time { return now }
	for _, biz := range []string{"a", "b", "c"} {
		if _, err := c.BeginOperation(biz, "demo", OpStart); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
		if err := c.FinishOperation(biz, nil); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	ops := s.ListOperations()
	if len(ops) != 2 {
		t.Fatalf("retention limit ignored: %d records", len(ops))
	}
	for _, op := range ops {
		if op.BizID == "a" {
			t.Fatal("evicted the newest instead of the oldest record")
		}
	}
	// 新的 processing 记录会挤掉最旧的终态记录，但它自己不会被挤掉。
	if _, err := c.BeginOperation("d", "demo", OpStart); err != nil {
		t.Fatal(err)
	}
	ops = s.ListOperations()
	if len(ops) != 2 {
		t.Fatalf("retention limit ignored after insert: %d records", len(ops))
	}
	if _, err := s.GetOperation("d"); err != nil {
		t.Fatalf("evicted an in-flight operation: %v", err)
	}
}

func TestBeginOperationRejectsInvalidInput(t *testing.T) {
	c, _ := fixture(t)
	for name, tc := range map[string]struct{ biz, workspace, action string }{
		"empty biz_id":    {"", "demo", OpStart},
		"unsafe biz_id":   {"biz/../id", "demo", OpStart},
		"long biz_id":     {strings.Repeat("x", 65), "demo", OpStart},
		"empty workspace": {"biz-1", "", OpStart},
		"unknown action":  {"biz-2", "demo", "suspend"},
		"empty action":    {"biz-3", "demo", ""},
	} {
		if _, err := c.BeginOperation(tc.biz, tc.workspace, tc.action); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s was accepted: %v", name, err)
		}
	}
	if c.Metrics.OperationsStarted.Load() != 0 {
		t.Fatal("invalid submissions created records")
	}
}

func TestOperationSnapshotSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := New(s, &fakeRuntime{}, map[string]Profile{"demo": {}}, time.Minute)
	if _, err := c.BeginOperation("biz-durable", "demo", OpStop); err != nil {
		t.Fatal(err)
	}
	if err := c.FinishOperation("biz-durable", nil); err != nil {
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
	c = New(s, &fakeRuntime{}, map[string]Profile{"demo": {}}, time.Minute)
	if _, err := c.BeginOperation("biz-durable", "demo", OpStop); !errors.Is(err, ErrOperationSucceeded) {
		t.Fatalf("idempotency was lost across restart: %v", err)
	}
}
