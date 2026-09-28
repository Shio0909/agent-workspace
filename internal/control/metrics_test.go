package control

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func render(t *testing.T, c *Controller) string {
	t.Helper()
	var b strings.Builder
	if err := c.WriteMetrics(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// scrape 把 Prometheus 文本解析成 name{labels} -> value，断言因此针对指标
// 本身，而不是某一行文本的排版。
func scrape(t *testing.T, c *Controller) map[string]float64 {
	t.Helper()
	values := map[string]float64{}
	for _, line := range strings.Split(render(t, c), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("unexpected metric line %q", line)
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			t.Fatalf("metric %q: %v", line, err)
		}
		values[fields[0]] = value
	}
	return values
}

func TestMetricsDescribeTheReclamationChain(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started, c.IdleTimeout, c.StartupGrace, c.GracePeriod = now, time.Hour, 0, time.Hour
	ctx := context.Background()
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
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
	now = now.Add(2 * time.Hour)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	values := scrape(t, c)
	for name, want := range map[string]float64{
		`nc_workspaces{phase="deleted"}`: 1,
		"nc_expirations_total":           1,
		"nc_suspensions_total":           1,
		"nc_hard_deletes_total":          1,
		"nc_idle_stops_total":            0,
		"nc_reconcile_failures_total":    0,
		"nc_leases":                      0,
		"nc_audit_failures_total":        0,
	} {
		if got := values[name]; got != want {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	if values["nc_reconcile_total"] < 3 {
		t.Fatalf("reconciles were not counted: %v", values["nc_reconcile_total"])
	}
	if _, ok := values[`nc_workspaces{phase="running"}`]; ok {
		t.Fatal("phase series should disappear once no workspace has that phase")
	}
}

func TestMetricsCountIdleStopsAndLeases(t *testing.T) {
	c, _ := fixture(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.started, c.IdleTimeout = now.Add(-2*time.Hour), time.Hour
	ctx := context.Background()
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	c.now = func() time.Time { return now }
	if _, err := c.Reconcile(ctx, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(testActor, "held", "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "held", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	// 第一次对账创建运行时，第二次才会观察到就绪并进入 running。
	for i := 0; i < 2; i++ {
		if _, err := c.Reconcile(ctx, "held"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Lease(testActor, "held", "", 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	values := scrape(t, c)
	for name, want := range map[string]float64{
		"nc_idle_stops_total":            1,
		"nc_leases":                      1,
		`nc_workspaces{phase="stopped"}`: 1,
		`nc_workspaces{phase="running"}`: 1,
	} {
		if got := values[name]; got != want {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestMetricsCountOperationsByStatus(t *testing.T) {
	c, _ := fixture(t)
	if _, err := c.BeginOperation("biz-ok", "demo", OpStart); err != nil {
		t.Fatal(err)
	}
	if err := c.FinishOperation("biz-ok", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginOperation("biz-bad", "demo", OpStop); err != nil {
		t.Fatal(err)
	}
	if err := c.FinishOperation("biz-bad", errors.New("apply failed")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginOperation("biz-open", "demo", OpRestart); err != nil {
		t.Fatal(err)
	}
	values := scrape(t, c)
	for name, want := range map[string]float64{
		`nc_operations{status="success"}`:    1,
		`nc_operations{status="failed"}`:     1,
		`nc_operations{status="processing"}`: 1,
		"nc_operations_started_total":        3,
		"nc_operation_replays_total":         0,
	} {
		if got := values[name]; got != want {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	if _, err := c.BeginOperation("biz-ok", "demo", OpStart); !errors.Is(err, ErrOperationSucceeded) {
		t.Fatal(err)
	}
	if got := scrape(t, c)["nc_operation_replays_total"]; got != 1 {
		t.Fatalf("replays were not counted: %v", got)
	}
}

func TestMetricsTextIsWellFormedAndStable(t *testing.T) {
	c, _ := fixture(t)
	text := render(t, c)
	help, types := map[string]int{}, map[string]int{}
	for _, line := range strings.Split(text, "\n") {
		switch {
		case strings.HasPrefix(line, "# HELP "):
			help[strings.Fields(line)[2]]++
		case strings.HasPrefix(line, "# TYPE "):
			fields := strings.Fields(line)
			types[fields[2]]++
			if fields[3] != "counter" && fields[3] != "gauge" && fields[3] != "histogram" {
				t.Fatalf("unexpected metric type in %q", line)
			}
		}
	}
	for _, name := range []string{"nc_workspaces", "nc_operations", "nc_leases", "nc_reconcile_total",
		"nc_reconcile_failures_total", "nc_idle_stops_total", "nc_expirations_total", "nc_suspensions_total",
		"nc_hard_deletes_total", "nc_operations_started_total", "nc_operation_replays_total",
		"nc_operation_takeovers_total", "nc_audit_failures_total", "nc_event_reconciles_total",
		"nc_event_reconcile_failures_total", "nc_workspace_acquire_seconds", "nc_workspace_start_seconds"} {
		if help[name] != 1 || types[name] != 1 {
			t.Fatalf("%s is missing HELP/TYPE: help=%d type=%d", name, help[name], types[name])
		}
	}
	// 标签顺序固定，两次抓取可以直接比对。
	if again := render(t, c); again != text {
		t.Fatalf("two scrapes differ:\n%s\n%s", text, again)
	}
}
