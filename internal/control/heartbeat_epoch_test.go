package control

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func heartbeatResponse(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: r}
}

// beatWhile 让一次心跳停在"请求已发出、响应未返回"，在这个窗口里执行 during，
// 然后放行响应并等 Beat 返回。窗口里发生的事就是锁外 HTTP 调用期间的并发修改。
func beatWhile(t *testing.T, c *Controller, status int, body string, during func()) {
	t.Helper()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.HeartbeatClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return heartbeatResponse(r, status, body), nil
	})}
	go func() {
		c.Beat(context.Background(), "demo", "http://old-workload:8080")
		close(done)
	}()
	<-entered
	during()
	close(release)
	<-done
}

func beatFixture(t *testing.T) *Controller {
	t.Helper()
	c, _ := fixture(t)
	p := c.profiles["demo"]
	p.HeartbeatPath = "/heartbeat"
	c.profiles["demo"] = p
	c.HeartbeatMisses = 1
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	return c
}

func stopThenStart(t *testing.T, c *Controller) {
	t.Helper()
	if _, err := c.SetDesired(testActor, "demo", DesiredStopped); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "demo", DesiredRunning); err != nil {
		t.Fatal(err)
	}
}

func TestReportFromBeforeStopStartIsDiscarded(t *testing.T) {
	c := beatFixture(t)
	beatWhile(t, c, 200, `{"version":"old-v1","busy":true}`, func() { stopThenStart(t, c) })
	w, err := c.store.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w.AgentVersion != "" {
		t.Fatalf("a report from the previous run was applied after stop/start: agent_version=%q", w.AgentVersion)
	}
	if !w.HeartbeatAt.IsZero() {
		t.Fatalf("the new run was marked healthy by the previous run's answer: %v", w.HeartbeatAt)
	}
}

func TestReportFromBeforeRestartIsDiscarded(t *testing.T) {
	c := beatFixture(t)
	beatWhile(t, c, 200, `{"version":"old-v1"}`, func() {
		if _, err := c.Restart(testActor, "demo"); err != nil {
			t.Fatal(err)
		}
	})
	if w, _ := c.store.Get("demo"); w.AgentVersion != "" {
		t.Fatalf("a report from the replaced workload was applied: agent_version=%q", w.AgentVersion)
	}
}

func TestFailureFromBeforeStopStartDoesNotCondemnNewRun(t *testing.T) {
	c := beatFixture(t)
	beatWhile(t, c, 503, ``, func() { stopThenStart(t, c) })
	w, err := c.store.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w.Unresponsive || w.RestartPending {
		t.Fatalf("the old run's failure marked the new run unresponsive: unresponsive=%v restart_pending=%v", w.Unresponsive, w.RestartPending)
	}
	if n := c.Metrics.HeartbeatFailures.Load(); n != 0 {
		t.Fatalf("the old run's failure was counted against the new run: %d", n)
	}
}

// 工作区的其他写入（活动时间、用量落盘、预算之外的字段）不换代，否则每次
// 心跳期间只要有别的写入，结果就会被丢弃，心跳永远应用不上。
func TestReportIsAppliedWhenOnlyUnrelatedFieldsChangeMeanwhile(t *testing.T) {
	c := beatFixture(t)
	beatWhile(t, c, 200, `{"version":"v1"}`, func() {
		w, err := c.store.Get("demo")
		if err != nil {
			t.Fatal(err)
		}
		w.LastError, w.LastActivity = "transient", c.now()
		if err := c.store.Put(w); err != nil {
			t.Fatal(err)
		}
	})
	if w, _ := c.store.Get("demo"); w.AgentVersion != "v1" {
		t.Fatalf("an unrelated write made the heartbeat result get discarded: agent_version=%q", w.AgentVersion)
	}
}
