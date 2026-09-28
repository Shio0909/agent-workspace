package control

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHistogramsRenderAndObserveConcurrently(t *testing.T) {
	var h Histogram
	const observations = 100
	var wg sync.WaitGroup
	for i := 0; i < observations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h.Observe(float64(i) / 100)
		}(i)
	}
	wg.Wait()
	h.Observe(-1)

	var b bytes.Buffer
	h.writeTo(&b, "test_seconds", "Test seconds.", "", "")
	text := b.String()
	for _, want := range []string{
		"# TYPE test_seconds histogram",
		`test_seconds_bucket{le="+Inf"} 100`,
		"test_seconds_count 100",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("histogram output missing %q:\n%s", want, text)
		}
	}
	var sum float64
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "test_seconds_sum ") {
			if _, err := fmt.Sscanf(line, "test_seconds_sum %f", &sum); err != nil {
				t.Fatal(err)
			}
		}
	}
	if math.Abs(sum-49.5) > 1e-9 {
		t.Fatalf("histogram sum = %v, want 49.5:\n%s", sum, text)
	}
	if strings.Contains(text, "test_seconds_count{}") || strings.Contains(text, "test_seconds_sum{}") {
		t.Fatalf("unlabelled histogram emitted empty braces:\n%s", text)
	}
}

func TestColdStartPhaseMetrics(t *testing.T) {
	var m Metrics
	t0 := time.Unix(1000, 0)
	m.observeStartPhases(t0, StartupTimestamps{
		Scheduled:        t0.Add(200 * time.Millisecond),
		ContainerStarted: t0.Add(time.Second),
		Ready:            t0.Add(3 * time.Second),
	})

	var b bytes.Buffer
	for _, phase := range []struct {
		name string
		h    *Histogram
	}{
		{"schedule", &m.StartSchedule},
		{"pull", &m.StartPull},
		{"ready", &m.StartReady},
	} {
		phase.h.writeTo(&b, "test_phase_seconds", "Test phase.", "phase", phase.name)
	}
	text := b.String()
	for _, want := range []string{
		`test_phase_seconds_bucket{phase="schedule",le="0.25"} 1`,
		`test_phase_seconds_sum{phase="schedule"} 0.2`,
		`test_phase_seconds_bucket{phase="pull",le="1"} 1`,
		`test_phase_seconds_sum{phase="pull"} 0.8`,
		`test_phase_seconds_bucket{phase="ready",le="2.5"} 1`,
		`test_phase_seconds_sum{phase="ready"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("phase metrics missing %q:\n%s", want, text)
		}
	}
}

func TestColdStartPhaseMetricsHandleSecondPrecision(t *testing.T) {
	var m Metrics
	t0 := time.Unix(1000, 900*int64(time.Millisecond))
	m.observeStartPhases(t0, StartupTimestamps{
		Scheduled:        t0.Truncate(time.Second),
		ContainerStarted: t0.Truncate(time.Second),
		Ready:            t0.Truncate(time.Second),
	})
	for name, h := range map[string]*Histogram{
		"schedule": &m.StartSchedule,
		"pull":     &m.StartPull,
		"ready":    &m.StartReady,
	} {
		h.mu.Lock()
		count := h.count
		h.mu.Unlock()
		if count != 1 {
			t.Fatalf("%s count=%d, want 1", name, count)
		}
	}
}
