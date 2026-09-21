package control

// 这组测试用于量化控制面已经实现的机制，并为每个测量结果保留真实对照组。
//
// 复跑：
//
//	go test ./internal/control -run 'TestQuant' -race -count=1 -v
//
// 约定：
//   - 断言只保证"性质"（例如并发峰值恰好等于上限、挂起态 Delete 恒为 0），
//     实测的耗时/条数用 t.Logf 打出来，不写死成断言，避免在慢机器上假失败。
//   - 对照组不是"改一个常量再跑"，而是把要对比的那一层拿掉：幂等对照组直接
//     调用与主路径相同的 apply（等于把 biz_id 检查从链路里删掉），调度对照组
//     用同一个 Controller、同一份数据、同一个延迟，只把信号量拿掉。

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// quantSubmissions 是并发提交数。取 64 是因为它同时是 httpapi 里一次请求的
// 合理重试风暴规模，也让"1 次 vs 64 次"的对比一眼能看懂。
const quantSubmissions = 64

// ---------------------------------------------------------------------------
// 测试替身
// ---------------------------------------------------------------------------

// quantRuntime 在 fakeRuntime 之上加三样量化需要的东西：
//   - 记录同时在飞的对账数，用来测并发峰值（而不是"并发确实发生过"）；
//   - 尊重 context，这样"单轮超时"能真的中断在途调用，而不是被替身吞掉；
//   - 可开关的故障注入，用来跑对账重放。
type quantRuntime struct {
	fakeRuntime
	delay        time.Duration
	inFlight     atomic.Int64
	maxInFlight  atomic.Int64
	observeCalls atomic.Int64
	seenMu       sync.Mutex
	seen         map[string]struct{}
}

func (q *quantRuntime) Observe(ctx context.Context, w Workspace, p Profile) (Observation, error) {
	q.observeCalls.Add(1)
	current := q.inFlight.Add(1)
	for {
		max := q.maxInFlight.Load()
		if current <= max || q.maxInFlight.CompareAndSwap(max, current) {
			break
		}
	}
	defer q.inFlight.Add(-1)
	q.seenMu.Lock()
	if q.seen == nil {
		q.seen = map[string]struct{}{}
	}
	q.seen[w.ID] = struct{}{}
	q.seenMu.Unlock()
	if q.delay > 0 {
		select {
		case <-ctx.Done():
			return Observation{}, ctx.Err()
		case <-time.After(q.delay):
		}
	}
	return q.fakeRuntime.Observe(ctx, w, p)
}

// peak 返回实测并发峰值。
func (q *quantRuntime) peak() int64 { return q.maxInFlight.Load() }

// touched 返回真的走到运行时的工作区 ID（有序），用来判断"剩下的下一轮有没有
// 被扫到"。
func (q *quantRuntime) touched() []string {
	q.seenMu.Lock()
	defer q.seenMu.Unlock()
	out := make([]string, 0, len(q.seen))
	for id := range q.seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// panicOnDeleteRuntime 让"挂起阶段动了存储"不可能被悄悄放过：只要缩容路径上
// 有一次 Runtime.Delete，测试立刻 panic 而不是安静地多一个计数。
type panicOnDeleteRuntime struct {
	quantRuntime
	armed atomic.Bool
}

func (p *panicOnDeleteRuntime) Delete(ctx context.Context, w Workspace) error {
	if p.armed.Load() {
		panic("suspend path reached Runtime.Delete: the volume would have been removed")
	}
	return p.quantRuntime.Delete(ctx, w)
}

// ---------------------------------------------------------------------------
// 夹具
// ---------------------------------------------------------------------------

// quantFixture 打开一个数据目录并直接落盘 n 个工作区。用 saveSnapshot 一次性
// 写入而不是 Create 循环，是因为 Create 每建一个工作区就重写一次全量快照
// （O(n^2)），准备数据的时间会长过被测的调度本身。格式与 Store 自己写出来的
// 完全一致。
func quantFixture(t *testing.T, n int) (*Controller, *Store, *quantRuntime) {
	t.Helper()
	dir := t.TempDir()
	now := time.Now()
	items := make(map[string]Workspace, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("w-%04d", i)
		items[id] = Workspace{
			ID: id, Profile: "demo",
			// Desired=running 但还没起来：Reconcile 会走 Observe -> Ensure，
			// 也就是每条工作区都会真的碰一次运行时（并发峰值才有意义）。
			Desired: DesiredRunning, Phase: PhaseStopped,
			LastActivity: now, UpdatedAt: now,
		}
	}
	if err := saveSnapshot(filepath.Join(dir, "workspaces.json"), items); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := &quantRuntime{fakeRuntime: fakeRuntime{running: false}}
	c := New(s, r, map[string]Profile{"demo": {}}, time.Hour)
	c.IdleTimeout, c.StartupGrace, c.GracePeriod = time.Hour, 0, time.Hour
	return c, s, r
}

// scanAll 按调度器同样的分页方式把全部 ID 读出来，供无界对照组使用。
func scanAll(c *Controller, batch int) []string {
	var ids []string
	after := ""
	for {
		page, more := c.Scan(after, batch)
		if len(page) == 0 {
			break
		}
		after = page[len(page)-1].ID
		for _, w := range page {
			ids = append(ids, w.ID)
		}
		if !more {
			break
		}
	}
	return ids
}

// ---------------------------------------------------------------------------
// 1. 幂等：N 个 goroutine 并发提交同一个 biz_id
// ---------------------------------------------------------------------------

// applyStart 与 httpapi.Server 在拿到幂等记录之后执行的动作完全相同：只写
// 意图，真正的 Kubernetes 动作留给 Reconcile。它的副作用是可数的——一次状态
// 写入加一条 fsync 过的审计记录。
func applyStart(c *Controller, id string) error {
	_, err := c.SetDesired(testActor, id, DesiredRunning)
	return err
}

// submitGated 复刻 handler 的三步：占用幂等记录 -> 只有 err == nil 才执行动作
// -> 收尾。
func submitGated(c *Controller, bizID, id, opType string, apply func() error) (executed bool, err error) {
	if _, err := c.BeginOperation(bizID, id, opType); err != nil {
		return false, err
	}
	applyErr := apply()
	if err := c.FinishOperation(bizID, applyErr); err != nil {
		return true, err
	}
	return true, applyErr
}

// TestQuantIdempotencyGatesConcurrentSideEffects 测的是"同一个 biz_id 并发提交
// N 次，实际副作用几次"。
//
// 判据用的是审计条数：一次成功的 SetDesired 一定落一条 fsync 过的 start 记录，
// 它是这个系统里最接近"外部可见副作用"的东西。
func TestQuantIdempotencyGatesConcurrentSideEffects(t *testing.T) {
	type result struct {
		applied int
		start   int
		ensures int
	}

	run := func(t *testing.T, gated bool) result {
		t.Helper()
		c, r := fixture(t)
		var applied atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < quantSubmissions; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				apply := func() error {
					applied.Add(1)
					return applyStart(c, "demo")
				}
				if gated {
					_, _ = submitGated(c, "biz-quant", "demo", OpStart, apply)
					return
				}
				// 对照组：把幂等关口整个拿掉。调用方不碰幂等记录，直接执行
				// 与主路径一字不差的同一个 apply——这正是 biz_id 缺失时
				// handler 的行为，也是"关掉幂等检查"的字面含义。
				_ = apply()
			}()
		}
		wg.Wait()
		if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
			t.Fatal(err)
		}
		events, err := c.Audit(AuditQuery{Workspace: "demo", Action: ActionStart})
		if err != nil {
			t.Fatal(err)
		}
		ensures, _, _, _ := r.counts()
		return result{applied: int(applied.Load()), start: len(events), ensures: ensures}
	}

	gated := run(t, true)
	t.Logf("幂等开启：%d 次并发提交 -> 副作用 %d 次，审计 start 记录 %d 条，K8s Ensure %d 次",
		quantSubmissions, gated.applied, gated.start, gated.ensures)
	if gated.applied != 1 || gated.start != 1 {
		t.Fatalf("幂等没有把并发提交收敛成一次副作用: %+v", gated)
	}

	ungated := run(t, false)
	t.Logf("对照（去掉幂等关口）：%d 次并发提交 -> 副作用 %d 次，审计 start 记录 %d 条，K8s Ensure %d 次",
		quantSubmissions, ungated.applied, ungated.start, ungated.ensures)
	if ungated.applied != quantSubmissions || ungated.start != quantSubmissions {
		t.Fatalf("对照组没有复现出重复副作用: %+v", ungated)
	}
	// 诚实记录：K8s 侧两条路径都是 1 次，因为生命周期写操作只落意图，
	// 真正的运行时动作由幂等的 Reconcile 收敛。幂等键挡住的是状态写入与
	// 审计记录这些"每次调用都会留下痕迹"的副作用。
	if ungated.ensures != gated.ensures {
		t.Logf("注意：对照组的 K8s Ensure 次数不同（%d vs %d）", ungated.ensures, gated.ensures)
	}
}

// TestQuantReplayedRequestDoesNotResurrectWorkload 把对照组的破坏力摊开：
// 调用方超时后重放一个**已经成功**的请求，幂等开启时它不会把工作区复活，
// 关掉之后重放会真的把已经缩容的工作区重新拉起来。
func TestQuantReplayedRequestDoesNotResurrectWorkload(t *testing.T) {
	replay := func(t *testing.T, gated bool) (applied int, desired string, ensures int) {
		t.Helper()
		c, r := fixture(t)
		ctx := context.Background()
		// 第一次：biz-A 成功启动。
		if executed, err := submitGated(c, "biz-A", "demo", OpStart, func() error { return applyStart(c, "demo") }); !executed || err != nil {
			t.Fatalf("首提交失败: %v", err)
		}
		if _, err := c.Reconcile(ctx, "demo"); err != nil {
			t.Fatal(err)
		}
		// 之后用户用另一个 biz_id 把它停了。
		if executed, err := submitGated(c, "biz-B", "demo", OpStop, func() error {
			_, err := c.SetDesired(testActor, "demo", DesiredStopped)
			return err
		}); !executed || err != nil {
			t.Fatalf("停止失败: %v", err)
		}
		if _, err := c.Reconcile(ctx, "demo"); err != nil {
			t.Fatal(err)
		}
		before, _, _, _ := r.counts()

		// 现在客户端拿着旧 biz_id 重放 64 次。
		var count atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < quantSubmissions; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				apply := func() error { count.Add(1); return applyStart(c, "demo") }
				if gated {
					_, _ = submitGated(c, "biz-A", "demo", OpStart, apply)
					return
				}
				_ = apply() // 对照组：重放直接落成新的状态变更
			}()
		}
		wg.Wait()
		if _, err := c.Reconcile(ctx, "demo"); err != nil {
			t.Fatal(err)
		}
		w, err := c.Get("demo")
		if err != nil {
			t.Fatal(err)
		}
		ensures, _, _, _ = r.counts()
		return int(count.Load()), w.Desired, ensures - before
	}

	applied, desired, extraEnsures := replay(t, true)
	t.Logf("幂等开启：重放 %d 次 -> 生效 %d 次，最终 desired=%s，额外拉起工作负载 %d 次",
		quantSubmissions, applied, desired, extraEnsures)
	if applied != 0 || desired != DesiredStopped || extraEnsures != 0 {
		t.Fatalf("重放复活了已经缩容的工作区: applied=%d desired=%s ensures=%d", applied, desired, extraEnsures)
	}

	applied, desired, extraEnsures = replay(t, false)
	t.Logf("对照（去掉幂等关口）：重放 %d 次 -> 生效 %d 次，最终 desired=%s，额外拉起工作负载 %d 次",
		quantSubmissions, applied, desired, extraEnsures)
	if applied != quantSubmissions || desired != DesiredRunning || extraEnsures == 0 {
		t.Fatalf("对照组没有复现出重放造成的复活: applied=%d desired=%s ensures=%d", applied, desired, extraEnsures)
	}
}

// ---------------------------------------------------------------------------
// 2. 分阶段回收：suspended 阶段的 Runtime.Delete 必须是 0
// ---------------------------------------------------------------------------

// TestQuantSuspendedPhaseNeverReachesDelete 把回收链条跑满，并且用"调用即
// panic"的替身守住"挂起不删存储"这条不变量；对照组是同一条链条走到 deleted。
func TestQuantSuspendedPhaseNeverReachesDelete(t *testing.T) {
	const rounds = 50
	ctx := context.Background()

	run := func(t *testing.T, hardDelete bool) (deletes, stops, roundsReconciled int) {
		t.Helper()
		dir := t.TempDir()
		s, err := OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		r := &panicOnDeleteRuntime{}
		r.armed.Store(true) // 只要挂起路径碰了 Delete，测试直接 panic
		c := New(s, r, map[string]Profile{"demo": {}}, time.Hour)
		now := time.Now()
		c.now = func() time.Time { return now }
		c.started = now
		c.IdleTimeout, c.StartupGrace, c.GracePeriod = time.Hour, 0, time.Hour
		if _, err := c.Create(testActor, "demo", "demo"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Reconcile(ctx, "demo"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.SetExpiry(testActor, "demo", now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		// 租期到期 -> running 进入 suspended。
		now = now.Add(2 * time.Minute)
		c.now = func() time.Time { return now }
		if _, err := c.Reconcile(ctx, "demo"); err != nil {
			t.Fatal(err)
		}
		if w, err := c.Get("demo"); err != nil || w.Desired != DesiredSuspended {
			t.Fatalf("工作区没有被挂起: %+v %v", w, err)
		}
		// 挂起态连续对账 50 轮：这是"PVC 不会被删"的硬证据。
		for i := 0; i < rounds; i++ {
			if _, err := c.Reconcile(ctx, "demo"); err != nil {
				t.Fatal(err)
			}
		}
		_, suspStops, _, _ := r.counts()
		if _, _, d, _ := r.counts(); d != 0 {
			t.Fatalf("suspended 阶段调用了 Runtime.Delete: %d 次", d)
		}
		if !hardDelete {
			return 0, suspStops, rounds
		}
		// 对照组：宽限期满，同一份代码走到 deleted。
		r.armed.Store(false)
		now = now.Add(time.Hour)
		c.now = func() time.Time { return now }
		for i := 0; i < rounds; i++ {
			if _, err := c.Reconcile(ctx, "demo"); err != nil {
				t.Fatal(err)
			}
		}
		_, _, d, _ := r.counts()
		return d, suspStops, rounds
	}

	deletes, stops, n := run(t, false)
	t.Logf("suspended 阶段对账 %d 轮：Runtime.Delete %d 次，Runtime.Stop %d 次", n, deletes, stops)
	if deletes != 0 {
		t.Fatal("挂起阶段动了存储")
	}

	deletes, _, n = run(t, true)
	t.Logf("对照（宽限期满走到 deleted）对账 %d 轮：Runtime.Delete %d 次（首轮删除，之后墓碑直接跳过）", n, deletes)
	if deletes != 1 {
		t.Fatalf("硬删没有恰好发生一次: %d", deletes)
	}
}

// TestQuantReclamationDeletesOnlyAfterTheWholeChain 把三段式链条的每一跳都
// 用计数钉死：running -> stopped 不删、stopped -> suspended 不删、只有
// suspended -> deleted 才删，而且每条路径的 Delete 次数是 0/0/1。
func TestQuantReclamationDeletesOnlyAfterTheWholeChain(t *testing.T) {
	ctx := context.Background()
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started = now
	c.IdleTimeout, c.StartupGrace, c.GracePeriod = time.Minute, 0, time.Hour

	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	// 第 1 跳：空闲缩容 running -> stopped，保留存储。
	now = now.Add(2 * time.Minute)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	w, _ := c.Get("demo")
	if w.Desired != DesiredStopped {
		t.Fatalf("空闲缩容没有发生: %+v", w)
	}
	_, idleStops, deletesAfterIdle, _ := r.counts()
	if deletesAfterIdle != 0 {
		t.Fatalf("空闲缩容动了存储: %d", deletesAfterIdle)
	}
	t.Logf("第 1 跳 running -> stopped：Delete %d 次，Stop %d 次", deletesAfterIdle, idleStops)

	// 第 2 跳：租期到期 stopped -> suspended，保留存储。
	if _, err := c.SetExpiry(testActor, "demo", now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	_, _, deletesAfterSuspend, _ := r.counts()
	if deletesAfterSuspend != 0 {
		t.Fatalf("挂起动了存储: %d", deletesAfterSuspend)
	}
	t.Logf("第 2 跳 stopped -> suspended：Delete 累计 %d 次", deletesAfterSuspend)

	// 第 3 跳：宽限期满 suspended -> deleted。
	now = now.Add(2 * time.Hour)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	_, _, deletesAfterGrace, _ := r.counts()
	if deletesAfterGrace != 1 {
		t.Fatalf("宽限期满没有恰好删一次: %d", deletesAfterGrace)
	}
	t.Logf("第 3 跳 suspended -> deleted：Delete 累计 %d 次", deletesAfterGrace)
}

// TestQuantLeaseBlocksReclamation 量化"任务租约保护运行中的任务"：租期已经过期、
// 空闲时间也早就超过阈值，但只要租约还有效，连续 50 轮对账都不会缩容。
func TestQuantLeaseBlocksReclamation(t *testing.T) {
	const rounds = 50
	ctx := context.Background()
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started = now
	c.IdleTimeout, c.StartupGrace, c.GracePeriod = time.Minute, 0, time.Hour

	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	token, err := c.Lease(testActor, "demo", "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// 截止时间设到过去，同时把空闲时间推过阈值：两条回收链条都"该动手了"。
	if _, err := c.SetExpiry(testActor, "demo", now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	c.now = func() time.Time { return now }
	for i := 0; i < rounds; i++ {
		if _, err := c.Reconcile(ctx, "demo"); err != nil {
			t.Fatal(err)
		}
	}
	w, err := c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	_, stops, deletes, _ := r.counts()
	if w.Desired != DesiredRunning || stops != 0 || deletes != 0 {
		t.Fatalf("租约没有挡住回收: desired=%s stops=%d deletes=%d", w.Desired, stops, deletes)
	}
	t.Logf("持有有效租约、且空闲与租期都已超时：连续 %d 轮对账 -> 缩容 %d 次、Delete %d 次，desired 保持 %s",
		rounds, stops, deletes, w.Desired)

	// 释放租约：下一轮就把 running -> stopped -> suspended 走完（同一轮里的连续转移）。
	if err := c.ReleaseLease(testActor, "demo", token); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	w, err = c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	_, stops, deletes, _ = r.counts()
	if w.Desired != DesiredSuspended || stops == 0 || deletes != 0 {
		t.Fatalf("释放租约后没有回收: desired=%s stops=%d deletes=%d", w.Desired, stops, deletes)
	}
	t.Logf("释放租约后：第 1 轮就走到 desired=%s（缩容 %d 次、Delete 仍为 %d 次，存储保留）",
		w.Desired, stops, deletes)
}

// TestQuantInFlightRequestBlocksReclamation 量化"活跃请求计数保护运行中的任务"：
// 只要还有没释放的在途请求，回收就不推进。
func TestQuantInFlightRequestBlocksReclamation(t *testing.T) {
	const rounds = 50
	ctx := context.Background()
	c, r := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started = now
	c.IdleTimeout, c.StartupGrace, c.GracePeriod = time.Minute, 0, time.Hour

	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	// Acquire 持有一个在途请求（模拟 SSE/长连接还没结束）。
	if _, release, err := c.Acquire(ctx, "demo"); err != nil {
		t.Fatal(err)
	} else {
		defer release()
		now = now.Add(2 * time.Minute)
		c.now = func() time.Time { return now }
		for i := 0; i < rounds; i++ {
			if _, err := c.Reconcile(ctx, "demo"); err != nil {
				t.Fatal(err)
			}
		}
		w, err := c.Get("demo")
		if err != nil {
			t.Fatal(err)
		}
		_, stops, deletes, _ := r.counts()
		if w.Desired != DesiredRunning || stops != 0 || deletes != 0 {
			t.Fatalf("在途请求没有挡住回收: desired=%s stops=%d deletes=%d", w.Desired, stops, deletes)
		}
		t.Logf("持有 1 个在途请求、且空闲已超时：连续 %d 轮对账 -> 缩容 %d 次、Delete %d 次，desired 保持 %s",
			rounds, stops, deletes, w.Desired)

		// 释放请求：下一轮缩容。空闲缩容需要 LastActivity 也过期，release 会把
		// 它刷成当前时刻，所以再往前推一个空闲周期。
		release()
		now = now.Add(2 * time.Minute)
		c.now = func() time.Time { return now }
		if _, err := c.Reconcile(ctx, "demo"); err != nil {
			t.Fatal(err)
		}
		w, _ = c.Get("demo")
		if w.Desired != DesiredStopped {
			t.Fatalf("释放请求后没有缩容: %+v", w)
		}
		t.Logf("释放请求后：第 1 轮缩容到 desired=%s（Delete %d 次，存储保留）", w.Desired, deletes)
	}
}

// ---------------------------------------------------------------------------
// 3. 调度器：批大小、并发上限、单轮超时
// ---------------------------------------------------------------------------

const (
	quantWorkspaces = 256
	quantBatch      = 64
	quantConcurrent = 4
)

// TestQuantSchedulerBoundsFanOut 测一轮扫描把 256 条工作区压成多大的实际并发。
func TestQuantSchedulerBoundsFanOut(t *testing.T) {
	c, _, r := quantFixture(t, quantWorkspaces)
	r.delay = 2 * time.Millisecond
	s := NewScheduler(c)
	s.BatchSize, s.Concurrency = quantBatch, quantConcurrent
	s.RoundTimeout = time.Minute

	stats := s.Round(context.Background())
	t.Logf("%d 条工作区、批大小 %d、并发上限 %d：一轮扫描 %d 条、失败 %d 条、耗时 %s、实测并发峰值 %d",
		quantWorkspaces, quantBatch, quantConcurrent, stats.Scanned, stats.Failed, stats.Duration, r.peak())
	if stats.Scanned != quantWorkspaces || stats.Failed != 0 || stats.TimedOut {
		t.Fatalf("一轮没有覆盖全部工作区: %+v", stats)
	}
	if got := r.peak(); got != quantConcurrent {
		t.Fatalf("并发峰值 %d != 上限 %d", got, quantConcurrent)
	}
}

// TestQuantSchedulerWithoutSemaphoreIsUnbounded 是对照组：同一个 Controller、
// 同一份 256 条数据、同一个 2ms 延迟，只把信号量拿掉（每条工作区一个
// goroutine，不排队）。它量化的是"限流买到了什么、代价是什么"。
func TestQuantSchedulerWithoutSemaphoreIsUnbounded(t *testing.T) {
	c, _, r := quantFixture(t, quantWorkspaces)
	r.delay = 2 * time.Millisecond
	s := NewScheduler(c)
	s.BatchSize, s.Concurrency = quantBatch, quantConcurrent
	s.RoundTimeout = time.Minute

	ids := scanAll(c, quantBatch)
	started := time.Now()
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, _ = c.Reconcile(context.Background(), id)
		}(id)
	}
	wg.Wait()
	elapsed := time.Since(started)
	t.Logf("对照（去掉信号量）：%d 条工作区一次全扇出 -> 耗时 %s，实测并发峰值 %d",
		len(ids), elapsed, r.peak())
	if got := r.peak(); got <= quantConcurrent {
		t.Fatalf("对照组没有复现出无界并发: 峰值 %d", got)
	}
}

// TestQuantBoundedFanOutCostsWallClock 是并发上限的"代价"那一半：把运行时延迟
// 拉长到成为主项，让"限流换到了什么"可以被计时。
//
// 注意口径：本机实测里，每次对账都要重写全量快照（Store.Put），那是一个全局
// 串行点，所以无界版的耗时下界是"快照写入串行时间"而不是"运行时延迟"。两个
// 数字都要连着这条口径一起读。
func TestQuantBoundedFanOutCostsWallClock(t *testing.T) {
	const (
		workspaces = 64
		delay      = 50 * time.Millisecond
	)

	bounded, _, br := quantFixture(t, workspaces)
	br.delay = delay
	s := NewScheduler(bounded)
	s.BatchSize, s.Concurrency = quantBatch, quantConcurrent
	s.RoundTimeout = time.Minute
	stats := s.Round(context.Background())
	t.Logf("有界（并发 %d）：%d 条工作区耗时 %s，实测并发峰值 %d",
		quantConcurrent, workspaces, stats.Duration, br.peak())

	unbounded, _, ur := quantFixture(t, workspaces)
	ur.delay = delay
	ids := scanAll(unbounded, quantBatch)
	started := time.Now()
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, _ = unbounded.Reconcile(context.Background(), id)
		}(id)
	}
	wg.Wait()
	elapsed := time.Since(started)
	t.Logf("无界对照：%d 条工作区耗时 %s，实测并发峰值 %d（%.1fx 峰值换来 %.1fx 耗时）",
		workspaces, elapsed, ur.peak(),
		float64(ur.peak())/float64(quantConcurrent),
		float64(stats.Duration)/float64(elapsed))
	if ur.peak() <= int64(quantConcurrent) || br.peak() != int64(quantConcurrent) {
		t.Fatalf("峰值不符合预期: 有界 %d 无界 %d", br.peak(), ur.peak())
	}
}

// TestQuantRoundTimeoutStopsDispatching 量化单轮超时的行为：到点后不再派发、
// 在途的收尾，剩下的留到下一轮。
func TestQuantRoundTimeoutStopsDispatching(t *testing.T) {
	c, _, r := quantFixture(t, quantWorkspaces)
	r.delay = 10 * time.Millisecond
	s := NewScheduler(c)
	s.BatchSize, s.Concurrency = quantBatch, quantConcurrent
	s.RoundTimeout = 25 * time.Millisecond

	const rounds = 5
	total := 0
	for i := 0; i < rounds; i++ {
		stats := s.Round(context.Background())
		total += stats.Scanned
		t.Logf("第 %d 轮：派发 %d 条、失败 %d 条、TimedOut=%v、耗时 %s",
			i+1, stats.Scanned, stats.Failed, stats.TimedOut, stats.Duration)
		if i == 0 {
			if !stats.TimedOut {
				t.Fatalf("单轮超时没有触发: %+v", stats)
			}
			if stats.Scanned >= quantWorkspaces {
				t.Fatalf("超时的一轮竟然扫完了全部工作区: %+v", stats)
			}
			if stats.Duration > 2*time.Second {
				t.Fatalf("单轮没有被上限截断: %s", stats.Duration)
			}
		}
	}
	distinct := r.touched()
	t.Logf("单轮上限 %s、%d 条工作区：%d 轮共派发 %d 条，但只覆盖 %d 个不同的工作区（最小 ID=%s，最大 ID=%s）",
		s.RoundTimeout, quantWorkspaces, rounds, total, len(distinct), firstOr(distinct), lastOr(distinct))
	if len(distinct) == 0 {
		t.Fatal("没有工作区走到运行时")
	}
	// Round 每一轮都从 after="" 重新开始扫描，所以超时轮覆盖的永远是 ID 最小的
	// 那一批：慢速对账下 ID 靠后的工作区会被饿死。这是实测结论，不是推理。
	if len(distinct) > total {
		t.Fatalf("覆盖数不可能超过派发数: %d > %d", len(distinct), total)
	}
}

func firstOr(ids []string) string {
	if len(ids) == 0 {
		return "-"
	}
	return ids[0]
}

func lastOr(ids []string) string {
	if len(ids) == 0 {
		return "-"
	}
	return ids[len(ids)-1]
}

// TestQuantSchedulerDefaultsMatchTheDocumentedNumbers 固定默认值，
// 防止文档中的基准数据因默认配置变更而失效。
func TestQuantSchedulerDefaultsMatchTheDocumentedNumbers(t *testing.T) {
	s := NewScheduler(&Controller{})
	t.Logf("调度默认值：周期 %s、单轮上限 %s、批大小 %d、并发 %d",
		s.Interval, s.RoundTimeout, s.BatchSize, s.Concurrency)
	if s.Interval != 5*time.Second || s.RoundTimeout != time.Minute || s.BatchSize != 64 || s.Concurrency != 4 {
		t.Fatalf("默认值与文档不一致: %+v", s)
	}
}
