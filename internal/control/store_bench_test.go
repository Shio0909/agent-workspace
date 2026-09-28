package control

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestStoreWriteCost measures durable Put cost against the number of stored
// workspaces. It is opt-in because it writes to disk for tens of seconds:
//
//	AGENT_WORKSPACE_STORE_BENCH=1 AGENT_WORKSPACE_STORE_BENCH_OUT=/tmp/store.jsonl \
//	  go test ./internal/control -run TestStoreWriteCost -count=1 -v
//
// Sizes default to 100,1000,10000 (AGENT_WORKSPACE_STORE_BENCH_SIZES). Each size
// reports sequential latency and the throughput of 64 concurrent writers on
// distinct workspaces, which is what a scheduler round produces.
func TestStoreWriteCost(t *testing.T) {
	if os.Getenv("AGENT_WORKSPACE_STORE_BENCH") == "" {
		t.Skip("set AGENT_WORKSPACE_STORE_BENCH=1 to run")
	}
	sizes := []int{100, 1000, 10000}
	if raw := os.Getenv("AGENT_WORKSPACE_STORE_BENCH_SIZES"); raw != "" {
		sizes = nil
		for _, f := range strings.Split(raw, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil {
				t.Fatal(err)
			}
			sizes = append(sizes, n)
		}
	}
	label := os.Getenv("AGENT_WORKSPACE_STORE_BENCH_LABEL")
	var out *os.File
	if path := os.Getenv("AGENT_WORKSPACE_STORE_BENCH_OUT"); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		out = f
	}
	for _, n := range sizes {
		r := measureStoreWrites(t, n)
		r.Label = label
		t.Logf("%s n=%d seq p50=%.2fms p99=%.2fms (%d writes)  concurrent=%d: %.0f writes/s",
			label, n, r.SeqP50Ms, r.SeqP99Ms, r.SeqWrites, r.Writers, r.ConcurrentPerSec)
		if out != nil {
			b, _ := json.Marshal(r)
			fmt.Fprintln(out, string(b))
		}
	}
}

type storeWriteResult struct {
	Label            string  `json:"label"`
	Workspaces       int     `json:"workspaces"`
	SeqWrites        int     `json:"seq_writes"`
	SeqP50Ms         float64 `json:"seq_p50_ms"`
	SeqP99Ms         float64 `json:"seq_p99_ms"`
	Writers          int     `json:"writers"`
	ConcurrentWrites int     `json:"concurrent_writes"`
	ConcurrentPerSec float64 `json:"concurrent_per_sec"`
}

func measureStoreWrites(t *testing.T, n int) storeWriteResult {
	t.Helper()
	dir := t.TempDir()
	now := time.Now()
	items := make(map[string]Workspace, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("w-%05d", i)
		items[id] = Workspace{ID: id, Profile: "demo", Desired: DesiredRunning, Phase: PhaseRunning,
			LastActivity: now, UpdatedAt: now}
	}
	if err := saveSnapshot(filepath.Join(dir, "workspaces.json"), items); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Sequential: a fixed time budget, so a slow backend is not measured on a
	// handful of samples only because it is slow.
	var lat []time.Duration
	deadline := time.Now().Add(3 * time.Second)
	for i := 0; time.Now().Before(deadline) || len(lat) < 20; i++ {
		w := items[fmt.Sprintf("w-%05d", i%n)]
		w.UpdatedAt = time.Now()
		start := time.Now()
		if err := s.Put(w); err != nil {
			t.Fatal(err)
		}
		lat = append(lat, time.Since(start))
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) float64 {
		return float64(lat[int(p*float64(len(lat)-1))].Microseconds()) / 1000
	}

	const writers = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	start := time.Now()
	stop := start.Add(3 * time.Second)
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			done := 0
			for i := g; time.Now().Before(stop) || done == 0; i += writers {
				w := items[fmt.Sprintf("w-%05d", i%n)]
				w.UpdatedAt = time.Now()
				if err := s.Put(w); err != nil {
					t.Error(err)
					return
				}
				done++
			}
			mu.Lock()
			total += done
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(start)
	return storeWriteResult{Workspaces: n, SeqWrites: len(lat), SeqP50Ms: pct(0.50), SeqP99Ms: pct(0.99),
		Writers: writers, ConcurrentWrites: total, ConcurrentPerSec: float64(total) / elapsed.Seconds()}
}
