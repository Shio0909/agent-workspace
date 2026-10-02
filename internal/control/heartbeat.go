package control

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// heartbeatReport is what a workload returns from its heartbeat path. Both
// fields are optional: an empty 200 still proves the process is answering.
type heartbeatReport struct {
	// Busy means work is in flight that no client request is holding open, such
	// as an agent finishing a task. It counts as activity, so the idle reaper
	// does not stop a workspace in the middle of it.
	Busy    bool   `json:"busy"`
	Version string `json:"version"`
}

// Beat polls the workload of a running workspace. The scheduler calls it after
// every reconcile; endpoint is the address Reconcile returned, which is empty
// while the workspace is not ready.
//
// Three outcomes matter:
//   - busy keeps the workspace from being reclaimed as idle;
//   - HeartbeatMisses failures in a row mark it unresponsive and request a
//     restart (rate-limited by RestartCooldown);
//   - a later success clears the flag.
//
// A hung process stops passing its readiness probe long before the heartbeat
// fails, so a workspace that was healthy a moment ago is still polled while it
// is not ready. A workspace that has not been healthy recently, or that is
// being rolled out or restarted, is not: a pod that is still starting is slow,
// not dead, and counting its misses would restart it in a loop.
//
// The HTTP call is made without the workspace lock: the request path takes the
// same lock, and a hung workload must not stall it for the poll's timeout.
func (c *Controller) Beat(ctx context.Context, id, endpoint string) {
	s := c.slot(id)
	s.mu.Lock()
	w, err := c.store.Get(id)
	now := c.now()
	p, ok := c.profileFor(w)
	if err != nil || !ok || p.HeartbeatPath == "" || w.Desired != DesiredRunning || now.Before(s.nextBeat) {
		s.mu.Unlock()
		return
	}
	if endpoint == "" && !c.recentlyHealthy(w, now) {
		s.mu.Unlock()
		return
	}
	s.nextBeat = now.Add(c.HeartbeatInterval)
	s.mu.Unlock()

	if endpoint == "" {
		obs, err := c.runtime.Observe(ctx, w, p)
		if err != nil || obs.Endpoint == "" {
			return
		}
		endpoint = obs.Endpoint
	}
	report, status, pollErr := c.poll(ctx, strings.TrimRight(endpoint, "/")+p.HeartbeatPath)

	s.mu.Lock()
	defer s.mu.Unlock()
	w, err = c.store.Get(id)
	if err != nil || w.Desired != DesiredRunning {
		return
	}
	now = c.now()
	switch {
	case pollErr == nil:
		c.beatOK(s, &w, report, now)
	case status >= 400 && status < 500:
		// The workload answered but does not serve this path. That is a
		// misconfigured profile, not a sick workload; restarting would not fix
		// it and would repeat forever.
		c.Metrics.HeartbeatFailures.Add(1)
		slog.Warn("heartbeat path rejected", "workspace", id, "status", status)
	default:
		c.beatFailed(s, &w, pollErr, now)
	}
}

// recentlyHealthy reports whether a failure now would be a failure of a
// workload that was answering, rather than one that is still starting. The
// window covers the misses needed to give up plus the persistence delay of
// HeartbeatAt, and it closes for good once a restart is requested because the
// restart clears HeartbeatAt.
func (c *Controller) recentlyHealthy(w Workspace, now time.Time) bool {
	if w.HeartbeatAt.IsZero() || w.RolloutPending || w.Upgrade != nil || w.RestartPending {
		return false
	}
	window := time.Duration(c.HeartbeatMisses+1)*c.HeartbeatInterval + c.ActivityFlushInterval
	return now.Sub(w.HeartbeatAt) <= window
}

func (c *Controller) poll(ctx context.Context, url string) (heartbeatReport, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return heartbeatReport{}, 0, err
	}
	resp, err := c.HeartbeatClient.Do(req)
	if err != nil {
		return heartbeatReport{}, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return heartbeatReport{}, resp.StatusCode, fmt.Errorf("heartbeat returned status %d", resp.StatusCode)
	}
	var r heartbeatReport
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	_ = json.Unmarshal(body, &r) // an empty or non-JSON body is still a live process
	return r, resp.StatusCode, nil
}

func (c *Controller) beatOK(s *slot, w *Workspace, r heartbeatReport, now time.Time) {
	s.misses = 0
	if r.Busy && now.After(s.activity) {
		s.activity = now
	}
	changed := false
	if w.Unresponsive {
		w.Unresponsive, changed = false, true
		c.audit(ActorSystem, ActionHeartbeatBack, w.ID, "", ResultOK)
	}
	if v := truncate(r.Version, 64); v != w.AgentVersion {
		w.AgentVersion, changed = v, true
	}
	// Persist the timestamp only as often as request activity: a healthy
	// workspace must not cost a write per beat.
	if now.Sub(w.HeartbeatAt) >= c.ActivityFlushInterval {
		w.HeartbeatAt, changed = now, true
	}
	if changed {
		w.UpdatedAt = now
		if err := c.store.Put(*w); err != nil {
			slog.Warn("persist heartbeat", "workspace", w.ID, "error", err)
		}
	}
}

func (c *Controller) beatFailed(s *slot, w *Workspace, cause error, now time.Time) {
	c.Metrics.HeartbeatFailures.Add(1)
	s.misses++
	if s.misses < c.HeartbeatMisses {
		return
	}
	changed := false
	if !w.Unresponsive {
		w.Unresponsive, changed = true, true
		c.audit(ActorSystem, ActionHeartbeatLost, w.ID, fmt.Sprintf("%d consecutive failures: %s", s.misses, truncate(cause.Error(), 120)), ResultError)
	}
	if !w.RestartPending && now.Sub(w.AutoRestartAt) >= c.RestartCooldown {
		w.RestartPending, w.AutoRestartAt, changed = true, now, true
		w.HeartbeatAt = time.Time{} // the restarted pod starts a fresh window
		s.misses = 0
		s.resetReady()
		c.Metrics.HeartbeatRestarts.Add(1)
		c.audit(ActorSystem, ActionHeartbeatRestart, w.ID, "restart requested after lost heartbeats", ResultOK)
		c.queue.Add(w.ID)
	}
	if changed {
		w.UpdatedAt = now
		if err := c.store.Put(*w); err != nil {
			slog.Warn("persist heartbeat state", "workspace", w.ID, "error", err)
		}
	}
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
