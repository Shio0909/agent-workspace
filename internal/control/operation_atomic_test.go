package control

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFinishersBlocked 等 n 个 FinishOperation 调用都卡在 opsMu 上，用来把两次
// 收尾稳定地排成"同时到达、再依次获得锁"的交错。
func waitFinishersBlocked(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		blocked := 0
		for _, g := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(g, "(*Controller).FinishOperation(") && strings.Contains(g, "sync.Mutex.Lock") {
				blocked++
			}
		}
		if blocked >= n {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("finish calls did not reach the intended interleaving")
}

func TestConcurrentFinishKeepsFirstTerminal(t *testing.T) {
	c, _ := fixture(t)
	op, err := c.BeginOperation("atomic-finish", "demo", OpStop)
	if err != nil {
		t.Fatal(err)
	}
	c.store.opsMu.Lock()
	success, failure := make(chan error, 1), make(chan error, 1)
	go func() { success <- c.FinishOperation(op, nil) }()
	waitFinishersBlocked(t, 1)
	go func() { failure <- c.FinishOperation(op, errors.New("late failure")) }()
	waitFinishersBlocked(t, 2)
	c.store.opsMu.Unlock()
	if err := <-success; err != nil {
		t.Fatal(err)
	}
	if err := <-failure; err != nil {
		t.Fatal(err)
	}
	got, err := c.store.GetOperation("atomic-finish")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != OpSuccess || got.Error != "" {
		t.Fatalf("a concurrent late finish overwrote success: %+v", got)
	}
}

func TestStaleTakeoverCannotResurrectFinishedOperation(t *testing.T) {
	c, _ := fixture(t)
	old, err := c.BeginOperation("atomic-stale", "demo", OpStart)
	if err != nil {
		t.Fatal(err)
	}
	// 重试已经读到旧的 processing 快照，之后另一条请求先把记录收尾了。
	if err := c.FinishOperation(old, nil); err != nil {
		t.Fatal(err)
	}
	got, err := c.takeOverStale(old, old.StartedAt.Add(c.OperationLease+time.Second))
	if !errors.Is(err, ErrOperationSucceeded) {
		t.Fatalf("stale retry was not told the operation already succeeded: %v", err)
	}
	if got.Status != OpSuccess {
		t.Fatalf("the reply does not reflect the stored terminal record: %+v", got)
	}
	stored, _ := c.store.GetOperation("atomic-stale")
	if stored.Status != OpSuccess || stored.Generation != old.Generation {
		t.Fatalf("terminal record was rewritten by a stale takeover: %+v", stored)
	}
	if c.Metrics.OperationTakeovers.Load() != 0 {
		t.Fatal("a rejected takeover was counted")
	}
}

func TestTakeoverFromOutdatedSnapshotIsRejectedByGeneration(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.OperationLease = time.Minute
	first, err := c.BeginOperation("atomic-gen", "demo", OpStart)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	second, err := c.takeOverStale(first, now)
	if err != nil {
		t.Fatalf("takeover failed: %v", err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatalf("takeover did not start a new generation: %d -> %d", first.Generation, second.Generation)
	}
	// 另一条重试拿着同一份旧快照，在新一代的租约也过期之后才到达。租约检查
	// 拦不住它，只有代数不符才能说明它看到的不是当前这一代。
	now = now.Add(2 * time.Minute)
	got, err := c.takeOverStale(first, now)
	if !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("an outdated snapshot took over a newer generation: %v", err)
	}
	if got.Generation != second.Generation {
		t.Fatalf("reply does not describe the current generation: %+v", got)
	}
	if c.Metrics.OperationTakeovers.Load() != 1 {
		t.Fatalf("takeovers = %d, want 1", c.Metrics.OperationTakeovers.Load())
	}
}

func TestSupersededHolderCannotFinishTakenOverOperation(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.OperationLease = time.Minute
	original, err := c.BeginOperation("atomic-holder", "demo", OpStart)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	taker, err := c.BeginOperation("atomic-holder", "demo", OpStart)
	if err != nil {
		t.Fatalf("takeover failed: %v", err)
	}
	// 原持有者慢了一步才失败：它的结果不能给接管后仍在执行的操作定性。
	err = c.FinishOperation(original, errors.New("slow original failed"))
	if !errors.Is(err, ErrOperationSuperseded) {
		t.Fatalf("superseded finish was not reported: %v", err)
	}
	stored, _ := c.store.GetOperation("atomic-holder")
	if stored.Status != OpProcessing || stored.Error != "" || stored.Generation != taker.Generation {
		t.Fatalf("superseded holder changed the taken-over record: %+v", stored)
	}
	if err := c.FinishOperation(taker, nil); err != nil {
		t.Fatalf("current holder could not finish: %v", err)
	}
	stored, _ = c.store.GetOperation("atomic-holder")
	if stored.Status != OpSuccess {
		t.Fatalf("current holder's result was not recorded: %+v", stored)
	}
	// 记录进入终态后，旧持有者再来收尾只是重复收尾，不是错误。
	if err := c.FinishOperation(original, errors.New("later still")); err != nil {
		t.Fatalf("finish on a terminal record should be a no-op: %v", err)
	}
}

func TestConcurrentTakeoversOfOneStaleRecordHaveOneWinner(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.OperationLease = time.Minute
	if _, err := c.BeginOperation("atomic-race", "demo", OpStart); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	var winners atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.BeginOperation("atomic-race", "demo", OpStart); err == nil {
				winners.Add(1)
			} else if !errors.Is(err, ErrOperationInProgress) {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("takeover winners = %d, want exactly 1", winners.Load())
	}
	if c.Metrics.OperationTakeovers.Load() != 1 {
		t.Fatalf("takeovers = %d, want 1", c.Metrics.OperationTakeovers.Load())
	}
}

// 升级前落盘的 processing 记录没有 generation 字段（读出为 0）。它仍然必须能
// 被接管，接管后的持有者必须能正常收尾。
func TestRecordWithoutGenerationCanStillBeTakenOverAndFinished(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.OperationLease = time.Minute
	legacy := Operation{BizID: "atomic-legacy", Workspace: "demo", Type: OpStart, Status: OpProcessing, StartedAt: now}
	if _, _, err := c.store.putOperationIfAbsent(legacy); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	taken, err := c.BeginOperation("atomic-legacy", "demo", OpStart)
	if err != nil {
		t.Fatalf("legacy record was not taken over: %v", err)
	}
	if taken.Generation != 1 {
		t.Fatalf("generation after takeover = %d, want 1", taken.Generation)
	}
	if err := c.FinishOperation(taken, nil); err != nil {
		t.Fatal(err)
	}
	if stored, _ := c.store.GetOperation("atomic-legacy"); stored.Status != OpSuccess {
		t.Fatalf("legacy record did not finish: %+v", stored)
	}
}

func TestGenerationSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c := New(s, &fakeRuntime{}, map[string]Profile{"demo": {}}, time.Minute)
	c.now = func() time.Time { return now }
	c.OperationLease = time.Minute
	if _, err := c.BeginOperation("atomic-durable", "demo", OpStart); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	taken, err := c.BeginOperation("atomic-durable", "demo", OpStart)
	if err != nil {
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
	stored, err := s.GetOperation("atomic-durable")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Generation != taken.Generation || stored.Generation < 2 {
		t.Fatalf("generation was not persisted: stored=%d taken=%d", stored.Generation, taken.Generation)
	}
}
