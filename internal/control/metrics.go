package control

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
)

// Metrics 是控制面的进程内计数。这里只放单调计数器；工作区、租约和幂等记录
// 的数量在抓取时从快照算出来，避免同时维护两套会漂移的状态。New 保证它非 nil。
type Metrics struct {
	Reconciles         atomic.Int64
	ReconcileErrors    atomic.Int64
	IdleStops          atomic.Int64
	Expirations        atomic.Int64
	Suspensions        atomic.Int64
	HardDeletes        atomic.Int64
	OperationsStarted  atomic.Int64
	OperationReplays   atomic.Int64
	OperationTakeovers atomic.Int64
	AuditFailures      atomic.Int64
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
	writeValue(&b, "nc_operations_started_total", "Idempotency records created by a first submission.", "counter", m.OperationsStarted.Load())
	writeValue(&b, "nc_operation_replays_total", "Submissions which reused an existing biz_id.", "counter", m.OperationReplays.Load())
	writeValue(&b, "nc_operation_takeovers_total", "Stale processing records taken over by a later submission.", "counter", m.OperationTakeovers.Load())
	writeValue(&b, "nc_audit_failures_total", "Audit events which could not be persisted.", "counter", m.AuditFailures.Load())
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
