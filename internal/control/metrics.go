package control

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics 是控制面的进程内计数。这里只放单调计数器和固定桶直方图；工作区、
// 租约和幂等记录的数量在抓取时从快照算出来，避免同时维护两套会漂移的状态。
// New 保证它非 nil。
type Metrics struct {
	Reconciles         atomic.Int64
	ReconcileErrors    atomic.Int64
	IdleStops          atomic.Int64
	Expirations        atomic.Int64
	Suspensions        atomic.Int64
	HardDeletes        atomic.Int64
	UpgradeCommits     atomic.Int64
	UpgradeRollbacks   atomic.Int64
	HeartbeatFailures  atomic.Int64
	HeartbeatRestarts  atomic.Int64
	OperationsStarted  atomic.Int64
	OperationReplays   atomic.Int64
	OperationTakeovers atomic.Int64
	AuditFailures      atomic.Int64
	EventReconciles    atomic.Int64
	// EventReconcileErrors 只统计事件通道里的失败；周期调度的失败已经由
	// ReconcileErrors 覆盖。
	EventReconcileErrors atomic.Int64
	// AcquireWait 是请求等到可用 endpoint 的耗时（热路径接近 0，冷启动是
	// 用户真实感知到的等待）。Start* 把冷启动按阶段拆开。
	AcquireWait   Histogram
	StartSchedule Histogram
	StartPull     Histogram
	StartReady    Histogram
	StartTotal    Histogram

	// 这两个计数器按 profile 分组而不是按工作区：工作区 ID 的数量没有上限，
	// 作为标签会把时序数撑爆，而 profile 由配置文件限定。
	usageMu        sync.Mutex
	tokens         map[tokenSeries]int64
	budgetSuspends map[string]int64
}

type tokenSeries struct{ profile, kind string }

// addTokens attributes the positive increments a heartbeat revealed.
func (m *Metrics) addTokens(profile string, prompt, completion int64) {
	m.usageMu.Lock()
	defer m.usageMu.Unlock()
	if m.tokens == nil {
		m.tokens = map[tokenSeries]int64{}
	}
	if prompt > 0 {
		m.tokens[tokenSeries{profile, "prompt"}] += prompt
	}
	if completion > 0 {
		m.tokens[tokenSeries{profile, "completion"}] += completion
	}
}

func (m *Metrics) addBudgetSuspension(profile string) {
	m.usageMu.Lock()
	defer m.usageMu.Unlock()
	if m.budgetSuspends == nil {
		m.budgetSuspends = map[string]int64{}
	}
	m.budgetSuspends[profile]++
}

func (m *Metrics) writeUsage(b *bytes.Buffer) {
	m.usageMu.Lock()
	defer m.usageMu.Unlock()
	series := make([]tokenSeries, 0, len(m.tokens))
	for k := range m.tokens {
		series = append(series, k)
	}
	sort.Slice(series, func(i, j int) bool {
		if series[i].profile != series[j].profile {
			return series[i].profile < series[j].profile
		}
		return series[i].kind < series[j].kind
	})
	fmt.Fprint(b, "# HELP nc_tokens_total LLM tokens reported by workloads through the heartbeat, by profile and kind.\n# TYPE nc_tokens_total counter\n")
	for _, k := range series {
		fmt.Fprintf(b, "nc_tokens_total{profile=%q,kind=%q} %d\n", k.profile, k.kind, m.tokens[k])
	}
	profiles := make([]string, 0, len(m.budgetSuspends))
	for p := range m.budgetSuspends {
		profiles = append(profiles, p)
	}
	sort.Strings(profiles)
	fmt.Fprint(b, "# HELP nc_budget_suspensions_total Workspaces suspended because they used up their token budget, by profile.\n# TYPE nc_budget_suspensions_total counter\n")
	for _, p := range profiles {
		fmt.Fprintf(b, "nc_budget_suspensions_total{profile=%q} %d\n", p, m.budgetSuspends[p])
	}
}

// histogramBuckets 覆盖从热路径（毫秒）到镜像拉取（分钟）的范围。
var histogramBuckets = []float64{0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}

// Histogram 是固定桶的直方图。手写而不是引入 Prometheus 客户端，理由和
// WriteMetrics 一样：零额外依赖，格式只有几行。
type Histogram struct {
	mu      sync.Mutex
	buckets []uint64
	count   uint64
	sum     float64
}

// Observe 记录一次秒数。负数样本（时钟混乱）直接丢弃，不让它污染桶。
func (h *Histogram) Observe(seconds float64) {
	if seconds < 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buckets == nil {
		h.buckets = make([]uint64, len(histogramBuckets)+1)
	}
	h.buckets[sort.SearchFloat64s(histogramBuckets, seconds)]++
	h.count++
	h.sum += seconds
}

// writeTo 以 Prometheus 直方图格式输出。label 为空时不带标签。
func (h *Histogram) writeTo(b *bytes.Buffer, name, help, label, value string) {
	h.writeToWithHeader(b, name, help, label, value, true)
}

// writeSamplesTo writes one labelled series without repeating the histogram
// family HELP/TYPE declarations, which must appear exactly once per family.
func (h *Histogram) writeSamplesTo(b *bytes.Buffer, name, label, value string) {
	h.writeToWithHeader(b, name, "", label, value, false)
}

func (h *Histogram) writeToWithHeader(b *bytes.Buffer, name, help, label, value string, header bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buckets == nil {
		h.buckets = make([]uint64, len(histogramBuckets)+1)
	}
	labels := ""
	if label != "" {
		labels = fmt.Sprintf("%s=%q,", label, value)
	}
	if header {
		fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	}
	var cumulative uint64
	for i, bound := range histogramBuckets {
		cumulative += h.buckets[i]
		fmt.Fprintf(b, "%s_bucket{%sle=%q} %d\n", name, labels, strconv.FormatFloat(bound, 'f', -1, 64), cumulative)
	}
	cumulative += h.buckets[len(histogramBuckets)]
	fmt.Fprintf(b, "%s_bucket{%sle=\"+Inf\"} %d\n", name, labels, cumulative)
	sum := strconv.FormatFloat(h.sum, 'f', -1, 64)
	if labels == "" {
		fmt.Fprintf(b, "%s_sum %s\n%s_count %d\n", name, sum, name, h.count)
		return
	}
	fmt.Fprintf(b, "%s_sum{%s} %s\n", name, trimComma(labels), sum)
	fmt.Fprintf(b, "%s_count{%s} %d\n", name, trimComma(labels), h.count)
}

func trimComma(s string) string {
	if len(s) > 0 && s[len(s)-1] == ',' {
		return s[:len(s)-1]
	}
	return s
}

// observeStartPhases 记录冷启动的分阶段耗时：调度、拉镜像+启动（合并段）、
// 就绪等待。只记录两端时间戳都存在的阶段，缺一个就跳过那一段而不是编造。
// 三段之和与 total 的起止点不同（base 被截断到秒、total 用控制器真实时钟），
// 不能直接相加比较，见 docs/cold-start.md。
func (m *Metrics) observeStartPhases(t0 time.Time, ts StartupTimestamps) {
	if ts.Scheduled.IsZero() {
		return
	}
	// Kubernetes condition timestamps are commonly second-granular while t0
	// comes from time.Now. Truncate the base to avoid a same-second timestamp
	// appearing to precede the start.
	base := t0.Truncate(time.Second)
	schedule, ok := phaseDuration(base, ts.Scheduled)
	if !ok {
		return
	}
	m.StartSchedule.Observe(schedule)
	if ts.ContainerStarted.IsZero() {
		return
	}
	pull, ok := phaseDuration(ts.Scheduled, ts.ContainerStarted)
	if !ok {
		return
	}
	m.StartPull.Observe(pull)
	if ts.Ready.IsZero() {
		return
	}
	ready, ok := phaseDuration(ts.ContainerStarted, ts.Ready)
	if !ok {
		return
	}
	m.StartReady.Observe(ready)
}

func phaseDuration(start, end time.Time) (float64, bool) {
	delta := end.Sub(start)
	if delta < 0 {
		// Allow one second of negative skew because API timestamps are often
		// truncated to whole seconds. Larger negative values indicate bad data.
		if delta < -time.Second {
			return 0, false
		}
		delta = 0
	}
	return delta.Seconds(), true
}

// WriteMetrics 以 Prometheus 文本格式导出指标。手写而不是引入客户端库：这里
// 只有单调计数和抓取时快照的 gauge，标准库足够，也保住了零依赖的约束。
func (c *Controller) WriteMetrics(out io.Writer) error {
	m := c.Metrics
	now := c.now()
	phases := map[string]int{}
	leases := 0
	for _, w := range c.store.List() {
		phases[w.Phase]++
		for _, expires := range w.Leases {
			if now.Before(expires) {
				leases++
			}
		}
	}
	statuses := map[string]int{}
	for _, op := range c.store.ListOperations() {
		statuses[op.Status]++
	}
	var b bytes.Buffer
	writeLabeled(&b, "nc_workspaces", "Workspaces by lifecycle phase.", "gauge", "phase", phases)
	writeLabeled(&b, "nc_operations", "Idempotency records by status.", "gauge", "status", statuses)
	writeValue(&b, "nc_leases", "Unexpired leases currently held by workspaces.", "gauge", int64(leases))
	writeValue(&b, "nc_reconcile_total", "Reconcile attempts, including those served from the ready endpoint cache.", "counter", m.Reconciles.Load())
	writeValue(&b, "nc_reconcile_failures_total", "Reconcile attempts which returned an error.", "counter", m.ReconcileErrors.Load())
	writeValue(&b, "nc_idle_stops_total", "Idle timeouts which turned running workspaces into stopped.", "counter", m.IdleStops.Load())
	writeValue(&b, "nc_expirations_total", "Deadlines which turned running or stopped workspaces into suspended.", "counter", m.Expirations.Load())
	writeValue(&b, "nc_suspensions_total", "Workspaces whose workload was scaled to zero with storage retained.", "counter", m.Suspensions.Load())
	writeValue(&b, "nc_hard_deletes_total", "Workspaces whose deployment, service and pvc were removed.", "counter", m.HardDeletes.Load())
	writeValue(&b, "nc_upgrade_commits_total", "Image upgrades which stayed ready long enough to be committed.", "counter", m.UpgradeCommits.Load())
	writeValue(&b, "nc_upgrade_rollbacks_total", "Image upgrades which did not become ready in time and were rolled back.", "counter", m.UpgradeRollbacks.Load())
	writeValue(&b, "nc_heartbeat_failures_total", "Heartbeat polls which failed.", "counter", m.HeartbeatFailures.Load())
	writeValue(&b, "nc_heartbeat_restarts_total", "Restarts triggered by lost heartbeats.", "counter", m.HeartbeatRestarts.Load())
	writeValue(&b, "nc_operations_started_total", "Idempotency records created by a first submission.", "counter", m.OperationsStarted.Load())
	writeValue(&b, "nc_operation_replays_total", "Submissions which reused an existing biz_id.", "counter", m.OperationReplays.Load())
	writeValue(&b, "nc_operation_takeovers_total", "Stale processing records taken over by a later submission.", "counter", m.OperationTakeovers.Load())
	writeValue(&b, "nc_audit_failures_total", "Audit events which could not be persisted.", "counter", m.AuditFailures.Load())
	writeValue(&b, "nc_event_reconciles_total", "Reconciles triggered by runtime events or intent changes.", "counter", m.EventReconciles.Load())
	writeValue(&b, "nc_event_reconcile_failures_total", "Event-driven reconciles which failed and were requeued with backoff.", "counter", m.EventReconcileErrors.Load())
	m.writeUsage(&b)
	m.AcquireWait.writeTo(&b, "nc_workspace_acquire_seconds", "Time a request waited for a ready endpoint, including cold starts.", "", "")
	// 注意 "pull" 的真实口径：Pod condition 时间戳不含 kubelet 的拉取事件，
	// 这一段实际是"拉镜像 + 容器启动"的合并耗时（PodScheduled → ContainerStarted），
	// 想单独拆出拉取需要消费 kube Event 资源，见 docs/cold-start.md。
	const startMetric = "nc_workspace_start_seconds"
	fmt.Fprintf(&b, "# HELP %s Cold-start duration by phase, from pod condition timestamps. The pull phase is image pull plus container start (pod conditions do not expose pull separately).\n# TYPE %s histogram\n", startMetric, startMetric)
	for _, phase := range []struct {
		name string
		h    *Histogram
	}{
		{"schedule", &m.StartSchedule},
		{"pull", &m.StartPull},
		{"ready", &m.StartReady},
		{"total", &m.StartTotal},
	} {
		phase.h.writeSamplesTo(&b, startMetric, "phase", phase.name)
	}
	// 运行时可附带自己的指标（比如对 API server 的请求数），用来证明缓存
	// 省掉了多少调用。
	if exporter, ok := c.runtime.(MetricsExporter); ok {
		if err := exporter.WriteMetrics(&b); err != nil {
			return err
		}
	}
	_, err := out.Write(b.Bytes())
	return err
}

func writeValue(b *bytes.Buffer, name, help, kind string, value int64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, kind, name, value)
}

// writeLabeled 按标签值排序输出，保证同一个进程的两次抓取可以直接比对。
func writeLabeled(b *bytes.Buffer, name, help, kind, label string, values map[string]int) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(b, "%s{%s=%q} %d\n", name, label, k, values[k])
	}
}
