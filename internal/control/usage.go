package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Usage is the cumulative LLM token consumption of one workspace. The workload
// owns the counter and persists it on its own volume; the controller only keeps
// the largest value it has seen, so a report can never lower it.
type Usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

// maxUsageTokens bounds every reported counter. 2^50 tokens is far beyond what
// one workspace can consume, and far enough from 2^63 that sums cannot overflow.
const maxUsageTokens = int64(1) << 50

// parseUsage validates the "usage" object of a heartbeat. Unknown fields are
// ignored. Anything else wrong with it (wrong type, negative, absurd) rejects
// the whole object: a half-trusted counter is worse than none, because the
// budget compares against it.
//
// total_tokens is raised to prompt + completion when it is smaller or absent,
// so a workload that reports only the two parts is still metered.
func parseUsage(raw json.RawMessage) (Usage, error) {
	var in struct {
		Prompt     *int64 `json:"prompt_tokens"`
		Completion *int64 `json:"completion_tokens"`
		Total      *int64 `json:"total_tokens"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return Usage{}, errors.New("usage must be an object whose token counts are integers")
	}
	var u Usage
	for _, f := range []struct {
		name string
		in   *int64
		out  *int64
	}{
		{"prompt_tokens", in.Prompt, &u.PromptTokens},
		{"completion_tokens", in.Completion, &u.CompletionTokens},
		{"total_tokens", in.Total, &u.TotalTokens},
	} {
		if f.in == nil {
			continue
		}
		if *f.in < 0 || *f.in > maxUsageTokens {
			return Usage{}, fmt.Errorf("%s out of range: %d", f.name, *f.in)
		}
		*f.out = *f.in
	}
	u.TotalTokens = max(u.TotalTokens, u.PromptTokens+u.CompletionTokens)
	return u, nil
}

func usageAbsent(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// atLeast returns the field-wise maximum.
func (u Usage) atLeast(o Usage) Usage {
	return Usage{
		PromptTokens:     max(u.PromptTokens, o.PromptTokens),
		CompletionTokens: max(u.CompletionTokens, o.CompletionTokens),
		TotalTokens:      max(u.TotalTokens, o.TotalTokens),
	}
}

func (w Workspace) overBudget() bool {
	return w.TokenBudget > 0 && w.Usage.TotalTokens >= w.TokenBudget
}

func budgetMessage(w Workspace) string {
	return fmt.Sprintf("token budget exceeded: total_tokens=%d, token_budget=%d", w.Usage.TotalTokens, w.TokenBudget)
}

// mergeUsage folds usage which has not been flushed yet into w. The caller must
// hold s.mu.
func mergeUsage(s *slot, w *Workspace) {
	w.Usage = w.Usage.atLeast(s.usage)
}

// observeUsage folds a heartbeat's usage into the slot and into w, and reports
// whether the persisted copy is due. The caller must hold s.mu.
//
// A report below the recorded value (a workload which lost its volume starts
// counting from zero) changes nothing and is audited once per episode. The
// tokens it consumed before catching up are not counted: the controller cannot
// tell a reset from a wrong number, and adding the new run on top of the old
// one would let one bad report inflate the total for good.
func (c *Controller) observeUsage(s *slot, w *Workspace, raw json.RawMessage, now time.Time) bool {
	if usageAbsent(raw) {
		return false
	}
	reported, err := parseUsage(raw)
	if err != nil {
		if !s.usageRejected {
			s.usageRejected = true
			slog.Warn("heartbeat usage rejected", "workspace", w.ID, "error", err)
		}
		return false
	}
	s.usageRejected = false
	mergeUsage(s, w)
	if reported.TotalTokens < w.Usage.TotalTokens {
		if !s.usageRegressed {
			s.usageRegressed = true
			c.audit(ActorSystem, ActionUsageRegressed, w.ID,
				fmt.Sprintf("reported total_tokens=%d is below the recorded %d; keeping the recorded value", reported.TotalTokens, w.Usage.TotalTokens), ResultError)
		}
	} else {
		s.usageRegressed = false
	}
	next := w.Usage.atLeast(reported)
	if next == w.Usage {
		return false
	}
	c.Metrics.addTokens(w.Profile, next.PromptTokens-w.Usage.PromptTokens, next.CompletionTokens-w.Usage.CompletionTokens)
	s.usage, w.Usage = next, next
	return now.Sub(s.usageFlushedAt) >= c.ActivityFlushInterval
}

// suspendOverBudget turns a running or stopped workspace into a suspended one
// and persists that before anything else happens, so a crash right after leaves
// the suspension in place and the next reconcile only has to stop the workload.
// The caller holds s.mu and must not persist w itself afterwards.
func (c *Controller) suspendOverBudget(s *slot, w *Workspace, now time.Time) error {
	w.Desired, w.SuspendedAt, w.SuspendedFor, w.UpdatedAt = DesiredSuspended, now, SuspendedForBudget, now
	w.LastError = budgetMessage(*w)
	if err := c.store.Put(*w); err != nil {
		return err
	}
	s.usageFlushedAt = now
	s.resetReady()
	c.Metrics.addBudgetSuspension(w.Profile)
	c.audit(ActorSystem, ActionBudgetExceeded, w.ID, w.LastError+", volume retained", ResultOK)
	c.queue.Add(w.ID)
	return nil
}

// SetTokenBudget sets or clears (0) the budget. Lowering it to or below the
// current usage suspends the workspace at once; raising or clearing it resumes
// a workspace which was suspended for its budget. It does not touch a
// workspace suspended because its lease ran out.
func (c *Controller) SetTokenBudget(actor, id string, budget int64) (Workspace, error) {
	if budget < 0 || budget > maxUsageTokens {
		return Workspace{}, fmt.Errorf("%w: token_budget must be in [0, %d]", ErrInvalid, maxUsageTokens)
	}
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return w, err
	}
	if w.Desired == DesiredDeleted {
		return w, ErrConflict
	}
	mergeUsage(s, &w)
	now := c.now()
	w.TokenBudget, w.UpdatedAt = budget, now
	action, detail := ActionBudgetSet, "token_budget="+formatBudget(budget)
	switch {
	case w.overBudget() && (w.Desired == DesiredRunning || w.Desired == DesiredStopped):
		c.audit(actor, action, id, detail, ResultOK)
		return w, c.suspendOverBudget(s, &w, now)
	case w.Desired == DesiredSuspended && w.SuspendedFor == SuspendedForBudget && !w.overBudget():
		w.SuspendedFor, w.LastError = "", ""
		if w.expired(now) {
			// It is suspended for the lease now; the grace period starts over
			// because the budget suspension never counted towards it.
			w.SuspendedAt = now
		} else {
			w.Desired, w.SuspendedAt = DesiredStopped, time.Time{}
			action, detail = ActionResume, "resumed with token_budget="+formatBudget(budget)
		}
	}
	if err := c.store.Put(w); err != nil {
		return w, err
	}
	c.audit(actor, action, id, detail, ResultOK)
	return w, nil
}

func formatBudget(budget int64) string {
	if budget == 0 {
		return "unlimited"
	}
	return fmt.Sprint(budget)
}
