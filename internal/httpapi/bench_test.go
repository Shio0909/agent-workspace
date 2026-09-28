package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-workspace/internal/control"
)

// benchResult 是一个并发档位的汇总。所有请求都计入，失败不重试也不剔除。
type benchResult struct {
	Label       string  `json:"label"`
	Concurrency int     `json:"concurrency"`
	Seconds     float64 `json:"seconds"`
	Requests    int     `json:"requests"`
	Errors      int     `json:"errors"`
	RPS         float64 `json:"rps"`
	P50ms       float64 `json:"p50_ms"`
	P95ms       float64 `json:"p95_ms"`
	P99ms       float64 `json:"p99_ms"`
	FirstError  string  `json:"first_error,omitempty"`
}

func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted))*p+0.5) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return float64(sorted[i]) / float64(time.Millisecond)
}

func benchLevels() []int {
	raw := os.Getenv("AGENT_WORKSPACE_BENCH_LEVELS")
	if raw == "" {
		raw = "20,100,200,500"
	}
	var out []int
	for _, part := range strings.Split(raw, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

// TestBenchGatewayHotPath drives the full warm path in one process: real TCP
// client -> gateway handler -> controller (on-disk store with fsync) -> real TCP
// upstream. The Server is built exactly as main.go builds it, so the default
// upstream transport is the one under test. It is opt-in because it is a
// measurement, not a correctness check.
func TestBenchGatewayHotPath(t *testing.T) {
	if os.Getenv("AGENT_WORKSPACE_BENCH") == "" {
		t.Skip("set AGENT_WORKSPACE_BENCH=1 to run")
	}
	duration := 5 * time.Second
	if raw := os.Getenv("AGENT_WORKSPACE_BENCH_DURATION"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			duration = d
		}
	}
	label := os.Getenv("AGENT_WORKSPACE_BENCH_LABEL")
	if label == "" {
		label = "current"
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	store, err := control.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	c := control.New(store, upstreamRuntime{upstream.URL}, map[string]control.Profile{"demo": {}}, 15*time.Minute)
	// Ablation knobs. Unset means the production defaults.
	if raw := os.Getenv("AGENT_WORKSPACE_BENCH_ACTIVITY_FLUSH"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatal(err)
		}
		c.ActivityFlushInterval = d
	}
	var transport http.RoundTripper
	if os.Getenv("AGENT_WORKSPACE_BENCH_TRANSPORT") == "stdlib" {
		transport = http.DefaultTransport
	}
	if _, err := c.Create("bench", "demo", "demo"); err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer((&Server{Controller: c, Token: "test-control-token", Actor: "api", Transport: transport}).Handler())
	defer gateway.Close()

	var results []benchResult
	for _, concurrency := range benchLevels() {
		client := &http.Client{Transport: &http.Transport{
			MaxIdleConns: concurrency, MaxIdleConnsPerHost: concurrency, IdleConnTimeout: 30 * time.Second,
		}}
		warm, _ := http.NewRequest("GET", gateway.URL+"/w/demo/health", nil)
		warm.Header.Set("X-Control-Token", "test-control-token")
		if resp, err := client.Do(warm); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}

		ctx, cancel := context.WithTimeout(context.Background(), duration)
		var mu sync.Mutex
		var latencies []time.Duration
		errorsTotal := 0
		firstError := ""
		var wg sync.WaitGroup
		started := time.Now()
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				local := make([]time.Duration, 0, 4096)
				localErrors := 0
				localFirst := ""
				for ctx.Err() == nil {
					req, _ := http.NewRequest("GET", gateway.URL+"/w/demo/health", nil)
					req.Header.Set("X-Control-Token", "test-control-token")
					at := time.Now()
					resp, err := client.Do(req)
					if err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						if resp.StatusCode != http.StatusOK {
							err = fmt.Errorf("status %d", resp.StatusCode)
						}
					}
					local = append(local, time.Since(at))
					if err != nil {
						localErrors++
						if localFirst == "" {
							localFirst = err.Error()
						}
					}
				}
				mu.Lock()
				latencies = append(latencies, local...)
				errorsTotal += localErrors
				if firstError == "" {
					firstError = localFirst
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
		elapsed := time.Since(started)
		cancel()
		client.CloseIdleConnections()
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		r := benchResult{Label: label, Concurrency: concurrency, Seconds: elapsed.Seconds(),
			Requests: len(latencies), Errors: errorsTotal, RPS: float64(len(latencies)) / elapsed.Seconds(),
			P50ms: percentile(latencies, 0.50), P95ms: percentile(latencies, 0.95), P99ms: percentile(latencies, 0.99), FirstError: firstError}
		results = append(results, r)
		t.Logf("%s c=%d requests=%d errors=%d rps=%.0f p50=%.2fms p95=%.2fms p99=%.2fms %s",
			label, concurrency, r.Requests, r.Errors, r.RPS, r.P50ms, r.P95ms, r.P99ms, r.FirstError)
	}
	if out := os.Getenv("AGENT_WORKSPACE_BENCH_OUT"); out != "" {
		f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for _, r := range results {
			if err := enc.Encode(r); err != nil {
				t.Fatal(err)
			}
		}
	}
}
