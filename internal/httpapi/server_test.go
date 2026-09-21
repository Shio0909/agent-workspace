package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent-workspace/internal/control"
)

type countingRuntime struct {
	upstreamRuntime
	observes atomic.Int64
	restarts atomic.Int64
	deletes  atomic.Int64
	mu       sync.Mutex
	probeErr error
}

func (u *countingRuntime) Observe(ctx context.Context, w control.Workspace, p control.Profile) (control.Observation, error) {
	u.observes.Add(1)
	return u.upstreamRuntime.Observe(ctx, w, p)
}

func (u *countingRuntime) Restart(context.Context, control.Workspace) error {
	u.restarts.Add(1)
	return nil
}

func (u *countingRuntime) Delete(context.Context, control.Workspace) error {
	u.deletes.Add(1)
	return nil
}

func (u *countingRuntime) Probe(context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.probeErr
}

func (u *countingRuntime) setProbeErr(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.probeErr = err
}

type testTransport func(*http.Request) (*http.Response, error)

func (tr testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return tr(r) }

func TestProxyFailureInvalidatesReadyEndpointWithoutReplay(t *testing.T) {
	for _, status := range []int{0, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			store, err := control.OpenStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			runtime := &countingRuntime{upstreamRuntime: upstreamRuntime{"http://agent.test"}}
			controller := control.New(store, runtime, map[string]control.Profile{"demo": {}}, time.Minute)
			if _, err := controller.Create("tester", "demo", "demo"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			server := &Server{Controller: controller, Token: "test-control-token", Transport: testTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				code := http.StatusOK
				if calls == 1 {
					if status == 0 {
						return nil, errors.New("connection refused")
					}
					code = status
				}
				return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
			})}
			handler := server.Handler()
			for i := 1; i <= 3; i++ {
				req := httptest.NewRequest("POST", "/w/demo/note", strings.NewReader("marker"))
				req.Header.Set("X-Control-Token", "test-control-token")
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, req)
				want := 200
				if i == 1 {
					want = status
					if want == 0 {
						want = 502
					}
				}
				if response.Code != want || calls != i {
					t.Fatalf("response=%d want=%d upstream calls=%d want=%d", response.Code, want, calls, i)
				}
			}
			if runtime.observes.Load() != 2 {
				t.Fatalf("expected revalidation then reuse, probes=%d", runtime.observes.Load())
			}
		})
	}
}

type upstreamRuntime struct{ url string }

func (u upstreamRuntime) Ensure(context.Context, control.Workspace, control.Profile) error {
	return nil
}
func (u upstreamRuntime) Stop(context.Context, control.Workspace) error    { return nil }
func (u upstreamRuntime) Delete(context.Context, control.Workspace) error  { return nil }
func (u upstreamRuntime) Restart(context.Context, control.Workspace) error { return nil }
func (u upstreamRuntime) Probe(context.Context) error                      { return nil }
func (u upstreamRuntime) Observe(context.Context, control.Workspace, control.Profile) (control.Observation, error) {
	return control.Observation{Exists: true, Ready: true, Endpoint: u.url}, nil
}

func newServer(t *testing.T, runtime control.Runtime, transport ...http.RoundTripper) (*control.Controller, http.Handler) {
	t.Helper()
	s, err := control.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	c := control.New(s, runtime, map[string]control.Profile{"demo": {}}, time.Minute)
	if _, err := c.Create("tester", "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	srv := &Server{Controller: c, Token: "test-control-token"}
	if len(transport) > 0 {
		srv.Transport = transport[0]
	}
	return c, srv.Handler()
}

func setup(t *testing.T, url string, transport ...http.RoundTripper) (*control.Controller, http.Handler) {
	t.Helper()
	return newServer(t, upstreamRuntime{url}, transport...)
}

// call 发一个带控制令牌的请求，写操作默认带上幂等键。
func call(t *testing.T, handler http.Handler, method, path, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("X-Control-Token", "test-control-token")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

// Real HTTP framing over net.Pipe, without OS sockets or external services.
type pipeListener struct {
	connections chan net.Conn
	done        chan struct{}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connections:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }
func (l *pipeListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}
func memoryTransport(t *testing.T, h http.Handler) *http.Transport {
	t.Helper()
	l := &pipeListener{connections: make(chan net.Conn), done: make(chan struct{})}
	s := &http.Server{Handler: h}
	go func() { _ = s.Serve(l) }()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		select {
		case l.connections <- server:
			return client, nil
		case <-ctx.Done():
			client.Close()
			server.Close()
			return nil, ctx.Err()
		case <-l.done:
			client.Close()
			server.Close()
			return nil, net.ErrClosed
		}
	}}
	t.Cleanup(func() {
		tr.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			_ = s.Close()
		}
	})
	return tr
}

func TestSSEIsForwardedBeforeCompletionAndHoldsActivity(t *testing.T) {
	done := make(chan struct{})
	upstream := memoryTransport(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" || r.URL.RawQuery != "q=1" {
			t.Error("proxy changed path/query")
		}
		if r.Header.Get("X-Control-Token") != "" {
			t.Error("leaked control token to runtime")
		}
		if r.Header.Get("Authorization") != "Bearer runtime-token" {
			t.Error("lost runtime authorization")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-done:
		case <-r.Context().Done():
		}
		fmt.Fprint(w, "data: last\n\n")
	}))
	c, handler := setup(t, "http://agent.test", upstream)
	client := &http.Client{Transport: memoryTransport(t, handler)}
	defer close(done)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://gateway.test/w/demo/events?q=1", nil)
	req.Header.Set("X-Control-Token", "test-control-token")
	req.Header.Set("Authorization", "Bearer runtime-token")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("not streaming: %q %v", line, err)
	}
	if _, err := c.SetDesired("tester", "demo", control.DesiredStopped); !errors.Is(err, control.ErrConflict) {
		t.Fatalf("active stream not protected: %v", err)
	}
}

func TestAuthAndStrictInput(t *testing.T) {
	_, handler := setup(t, "http://unused")
	for _, tc := range []struct {
		method, path, body, token, biz string
		status                         int
	}{
		{"GET", "/health", "", "", "", 200},
		{"GET", "/v1/workspaces", "", "", "", 401},
		{"POST", "/v1/workspaces", `{"id":"second","profile":"demo"}`, "test-control-token", "", 201},
		{"POST", "/v1/workspaces", `{"id":"third","profile":"demo","image":"arbitrary"}`, "test-control-token", "", 400},
		{"POST", "/v1/workspaces", `{"id":"third","profile":"demo"} {}`, "test-control-token", "", 400},
		{"POST", "/v1/workspaces/demo/stop", "", "test-control-token", "biz-stop-1", 202},
		// 写操作缺少幂等键：拒绝执行，而不是退化成"每次都会执行"。
		{"POST", "/v1/workspaces/demo/stop", "", "test-control-token", "", 400},
		{"POST", "/v1/workspaces/demo/start", "", "test-control-token", "", 400},
		{"DELETE", "/v1/workspaces/demo", "", "test-control-token", "", 400},
		{"POST", "/v1/workspaces/demo/restart", `{"biz_id":"biz-restart-1"}`, "test-control-token", "", 202},
		{"GET", "/v1/workspaces/missing", "", "test-control-token", "", 404},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("X-Control-Token", tc.token)
		if tc.biz != "" {
			req.Header.Set("X-Biz-Id", tc.biz)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Fatalf("%s %s got %d: %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}

func TestDuplicateBizIDAppliesOnce(t *testing.T) {
	c, handler := newServer(t, upstreamRuntime{"http://agent.test"})
	first := call(t, handler, "POST", "/v1/workspaces/demo/start", "", "X-Biz-Id", "biz-start-1")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first submission: %d %s", first.Code, first.Body.String())
	}
	if w, _ := c.Get("demo"); w.Desired != control.DesiredRunning {
		t.Fatalf("first submission did not apply: %+v", w)
	}
	// 把状态改回去，再重放同一个 biz_id：状态必须纹丝不动。
	if _, err := c.SetDesired("tester", "demo", control.DesiredStopped); err != nil {
		t.Fatal(err)
	}
	replay := call(t, handler, "POST", "/v1/workspaces/demo/start", "", "X-Biz-Id", "biz-start-1")
	if replay.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", replay.Code, replay.Body.String())
	}
	var op control.Operation
	if err := json.Unmarshal(replay.Body.Bytes(), &op); err != nil {
		t.Fatal(err)
	}
	if op.BizID != "biz-start-1" || op.Status != control.OpSuccess || op.Type != control.OpStart {
		t.Fatalf("replay did not return the stored record: %+v", op)
	}
	if w, _ := c.Get("demo"); w.Desired != control.DesiredStopped {
		t.Fatalf("replay applied a second side effect: %+v", w)
	}
	// 新的幂等键是一次新请求。
	if again := call(t, handler, "POST", "/v1/workspaces/demo/start", `{"biz_id":"biz-start-2"}`); again.Code != http.StatusAccepted {
		t.Fatalf("fresh biz_id: %d %s", again.Code, again.Body.String())
	}
	if w, _ := c.Get("demo"); w.Desired != control.DesiredRunning {
		t.Fatalf("fresh biz_id did not apply: %+v", w)
	}
}

func TestConcurrentDuplicateBizIDOverHTTP(t *testing.T) {
	c, handler := newServer(t, upstreamRuntime{"http://agent.test"})
	const submissions = 8
	var fresh, replays atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < submissions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recorder := call(t, handler, "POST", "/v1/workspaces/demo/restart", "", "X-Biz-Id", "biz-race")
			var body map[string]any
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Errorf("bad body: %v", err)
				return
			}
			switch recorder.Code {
			case http.StatusAccepted:
				// 首次执行返回工作区，进行中的重复提交返回幂等记录。
				if _, ok := body["id"]; ok {
					fresh.Add(1)
				} else {
					replays.Add(1)
				}
			case http.StatusOK:
				replays.Add(1)
			default:
				t.Errorf("unexpected status %d: %s", recorder.Code, recorder.Body.String())
			}
		}()
	}
	wg.Wait()
	if fresh.Load() != 1 {
		t.Fatalf("wanted exactly one executor, got %d (replays=%d)", fresh.Load(), replays.Load())
	}
	// 只有一次重启意图落盘，所以后续对账只会重启一次。
	if w, _ := c.Get("demo"); !w.RestartPending {
		t.Fatalf("restart intent is missing: %+v", w)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}
	if w, _ := c.Get("demo"); w.RestartPending {
		t.Fatal("duplicate submissions queued extra restarts")
	}
}

func TestHealthAndReadinessAreSeparate(t *testing.T) {
	runtime := &countingRuntime{upstreamRuntime: upstreamRuntime{"http://agent.test"}}
	controller, handler := newServer(t, runtime)
	controller.ReadyTTL = 0 // 关掉探针缓存，让状态变化立即可见
	probe := func(path string) int {
		t.Helper()
		return call(t, handler, "GET", path, "").Code
	}
	if got := probe("/ready"); got != http.StatusOK {
		t.Fatalf("ready with a healthy backend: %d", got)
	}
	runtime.setProbeErr(errors.New("cluster unreachable"))
	if got := probe("/ready"); got != http.StatusServiceUnavailable {
		t.Fatalf("ready with an unreachable backend: %d", got)
	}
	// 依赖不可用时进程仍是活的：这是把实例摘出流量而不是重启它的前提。
	if got := probe("/health"); got != http.StatusOK {
		t.Fatalf("liveness followed readiness: %d", got)
	}
	runtime.setProbeErr(nil)
	if got := probe("/ready"); got != http.StatusOK {
		t.Fatalf("ready did not recover: %d", got)
	}
}

func TestMetricsRequireTheControlToken(t *testing.T) {
	_, handler := setup(t, "http://unused")
	if got := call(t, handler, "GET", "/v1/workspaces", "").Code; got != http.StatusOK {
		t.Fatalf("unexpected status: %d", got)
	}
	req := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("metrics leaked without a token: %d", recorder.Code)
	}
	authorized := call(t, handler, "GET", "/metrics", "")
	if got := authorized.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Fatalf("unexpected metrics content type %q", got)
	}
	body := authorized.Body.String()
	for _, want := range []string{"nc_reconcile_total", "nc_workspaces", "# TYPE nc_leases gauge"} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body is missing %q:\n%s", want, body)
		}
	}
}

func TestExpiryAndRestartEndpoints(t *testing.T) {
	c, handler := newServer(t, upstreamRuntime{"http://agent.test"})
	before := time.Now()
	response := call(t, handler, "POST", "/v1/workspaces/demo/expiry", `{"expires_in_seconds":3600}`)
	if response.Code != http.StatusOK {
		t.Fatalf("expiry: %d %s", response.Code, response.Body.String())
	}
	w, err := c.Get("demo")
	if err != nil {
		t.Fatal(err)
	}
	if w.ExpiresAt.Before(before.Add(59*time.Minute)) || w.ExpiresAt.After(before.Add(61*time.Minute)) {
		t.Fatalf("deadline was not applied: %s", w.ExpiresAt)
	}
	if got := call(t, handler, "POST", "/v1/workspaces/demo/expiry", `{"expires_in_seconds":-1}`).Code; got != http.StatusBadRequest {
		t.Fatalf("negative expiry accepted: %d", got)
	}
	if got := call(t, handler, "POST", "/v1/workspaces/missing/expiry", `{"expires_in_seconds":60}`).Code; got != http.StatusNotFound {
		t.Fatalf("unknown workspace: %d", got)
	}
	if got := call(t, handler, "POST", "/v1/workspaces/demo/expiry", `{"expires_in_seconds":0}`).Code; got != http.StatusOK {
		t.Fatalf("clearing the deadline: %d", got)
	}
	if w, _ := c.Get("demo"); !w.ExpiresAt.IsZero() {
		t.Fatalf("deadline was not cleared: %s", w.ExpiresAt)
	}
	// 重启：意图落盘，运行时动作交给对账。
	if got := call(t, handler, "POST", "/v1/workspaces/demo/restart", "", "X-Biz-Id", "biz-r1").Code; got != http.StatusAccepted {
		t.Fatalf("restart: %d", got)
	}
	if w, _ := c.Get("demo"); !w.RestartPending || w.Desired != control.DesiredRunning {
		t.Fatalf("restart did not record intent: %+v", w)
	}
}

func TestAuditEndpointQueriesTheTrail(t *testing.T) {
	c, handler := newServer(t, upstreamRuntime{"http://agent.test"})
	if got := call(t, handler, "POST", "/v1/workspaces/demo/start", "", "X-Biz-Id", "biz-a1", "X-Actor", "alice").Code; got != http.StatusAccepted {
		t.Fatalf("start: %d", got)
	}
	if got := call(t, handler, "POST", "/v1/workspaces/demo/stop", "", "X-Biz-Id", "biz-a2").Code; got != http.StatusAccepted {
		t.Fatalf("stop: %d", got)
	}
	var events []control.AuditEvent
	body := call(t, handler, "GET", "/v1/audit?workspace=demo&action=start", "").Body
	if err := json.Unmarshal(body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Actor != "alice" || events[0].Action != control.ActionStart {
		t.Fatalf("unexpected audit query result: %+v", events)
	}
	// 默认归属来自服务端配置，而不是请求方自称。
	body = call(t, handler, "GET", "/v1/audit?action=stop", "").Body
	if err := json.Unmarshal(body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Actor != control.ActorUnknown {
		t.Fatalf("unexpected default actor: %+v", events)
	}
	if got := call(t, handler, "GET", "/v1/audit?since=yesterday", "").Code; got != http.StatusBadRequest {
		t.Fatalf("bad time filter accepted: %d", got)
	}
	if got := call(t, handler, "GET", "/v1/audit?limit=99999", "").Code; got != http.StatusBadRequest {
		t.Fatalf("unbounded limit accepted: %d", got)
	}
	body = call(t, handler, "GET", "/v1/audit?limit=1", "").Body
	if err := json.Unmarshal(body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("limit ignored: %d events", len(events))
	}
	if _, err := c.Get("demo"); err != nil {
		t.Fatal(err)
	}
}

func TestProxyRefusesExpiredWorkspace(t *testing.T) {
	_, handler := newServer(t, upstreamRuntime{"http://agent.test"})
	if got := call(t, handler, "POST", "/v1/workspaces/demo/expiry", `{"expires_in_seconds":1}`).Code; got != http.StatusOK {
		t.Fatalf("expiry: %d", got)
	}
	time.Sleep(1100 * time.Millisecond)
	// 代理路径必须在唤醒之前就拒绝：过期的工作区不能靠一次请求复活。
	response := call(t, handler, "GET", "/w/demo/note", "")
	if response.Code != http.StatusGone {
		t.Fatalf("expired workspace was proxied: %d %s", response.Code, response.Body.String())
	}
}

func TestFailedOperationReplayKeepsTheOriginalMessage(t *testing.T) {
	_, handler := newServer(t, upstreamRuntime{"http://agent.test"})
	// 未知工作区：动作失败，记录进入 failed 终态。
	if got := call(t, handler, "POST", "/v1/workspaces/ghost/start", "", "X-Biz-Id", "biz-g1").Code; got != http.StatusNotFound {
		t.Fatalf("unknown workspace: %d", got)
	}
	response := call(t, handler, "POST", "/v1/workspaces/ghost/start", "", "X-Biz-Id", "biz-g1")
	if response.Code != http.StatusConflict {
		t.Fatalf("replayed failure: %d %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "not found") {
		t.Fatalf("original error text was lost: %s", response.Body.String())
	}
	// 同一个幂等键换个动作：拒绝，而不是猜调用方想要什么。
	if got := call(t, handler, "POST", "/v1/workspaces/ghost/stop", "", "X-Biz-Id", "biz-g1").Code; got != http.StatusConflict {
		t.Fatalf("biz_id reuse across types: %d", got)
	}
}
