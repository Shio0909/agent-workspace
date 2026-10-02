package control

import (
	"fmt"
	"strings"
	"time"
)

const (
	DefaultUpgradeTimeout = 3 * time.Minute
	minUpgradeTimeout     = 10 * time.Second
	maxUpgradeTimeout     = 30 * time.Minute
)

// UpgradeState is an upgrade that has been requested and not yet decided.
// It is persisted, so a controller restart resumes the same verification
// instead of forgetting that the previous image is the one to fall back to.
type UpgradeState struct {
	From string `json:"from"`
	To   string `json:"to"`
	// Timeout is how long the workspace gets to become ready on the new image,
	// counted from the first reconcile that wants it running. Counting from
	// the request would let a stopped workspace "fail" an upgrade it never ran.
	Timeout    time.Duration `json:"timeout"`
	StartedAt  time.Time     `json:"started_at"`
	Deadline   time.Time     `json:"deadline,omitempty"`
	ReadySince time.Time     `json:"ready_since,omitempty"`
}

// UpgradeDone is the outcome of the most recent upgrade.
type UpgradeDone struct {
	From    string    `json:"from"`
	To      string    `json:"to"`
	Outcome string    `json:"outcome"` // committed | rolled-back | aborted
	Reason  string    `json:"reason,omitempty"`
	At      time.Time `json:"at"`
}

const (
	UpgradeCommitted  = "committed"
	UpgradeRolledBack = "rolled-back"
	UpgradeAborted    = "aborted"
)

// profileFor returns the profile a workspace runs with: its own, with the
// workspace's image override applied.
func (c *Controller) profileFor(w Workspace) (Profile, bool) {
	p, ok := c.profiles[w.Profile]
	if ok && w.Image != "" {
		p.Image = w.Image
	}
	return p, ok
}

func imageAllowed(p Profile, image string) bool {
	for _, pattern := range p.AllowedImages {
		if prefix, wild := strings.CutSuffix(pattern, "*"); wild {
			if strings.HasPrefix(image, prefix) {
				return true
			}
		} else if image == pattern {
			return true
		}
	}
	return false
}

// resetReady drops the cached ready endpoint. After an image change the cached
// address still points at the old pod for up to ReadyTTL.
func (s *slot) resetReady() { s.endpoint, s.readyUntil = "", time.Time{} }

// Upgrade moves a workspace to a new image and keeps the old one as the way
// back. It only records intent; Reconcile applies it and decides the outcome:
// committed once the workspace has stayed ready on the new image for
// UpgradeSettle, rolled back automatically if it does not get there before the
// deadline.
//
// Asking again for the same target is a no-op, so a retried request cannot
// restart the clock; asking for a different one while an upgrade is undecided
// is a conflict, because the fallback image would otherwise be lost.
func (c *Controller) Upgrade(actor, id, image string, timeout time.Duration) (Workspace, error) {
	if !imagePattern.MatchString(image) {
		return Workspace{}, fmt.Errorf("%w: invalid image reference", ErrInvalid)
	}
	if timeout == 0 {
		timeout = DefaultUpgradeTimeout
	}
	if timeout < minUpgradeTimeout || timeout > maxUpgradeTimeout {
		return Workspace{}, fmt.Errorf("%w: timeout must be between %s and %s", ErrInvalid, minUpgradeTimeout, maxUpgradeTimeout)
	}
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return Workspace{}, err
	}
	if w.Desired == DesiredDeleted {
		return Workspace{}, ErrConflict
	}
	base, ok := c.profiles[w.Profile]
	if !ok {
		return Workspace{}, fmt.Errorf("%w: unknown profile", ErrInvalid)
	}
	if !imageAllowed(base, image) {
		return Workspace{}, fmt.Errorf("%w: image is not allowed for profile %q", ErrInvalid, w.Profile)
	}
	if w.Upgrade != nil {
		if w.Upgrade.To == image {
			return w, nil
		}
		return Workspace{}, fmt.Errorf("%w: an upgrade to %s is still being verified", ErrConflict, w.Upgrade.To)
	}
	current, _ := c.profileFor(w)
	if current.Image == image {
		return w, nil
	}
	now := c.now()
	w.Upgrade = &UpgradeState{From: current.Image, To: image, Timeout: timeout, StartedAt: now}
	w.Image, w.RolloutPending, w.UpdatedAt = imageOverride(base, image), true, now
	if err := c.store.Put(w); err != nil {
		return Workspace{}, err
	}
	s.resetReady()
	c.audit(actor, ActionUpgrade, id, "from="+current.Image+" to="+image, ResultOK)
	c.queue.Add(id)
	return w, nil
}

// imageOverride stores nothing when the image is the profile's own, so the
// workspace follows the profile again.
func imageOverride(base Profile, image string) string {
	if image == base.Image {
		return ""
	}
	return image
}

// Rollback returns a workspace to its previous image. During an undecided
// upgrade it aborts the upgrade. After a committed one it goes back to the
// image that ran before it, which makes a second call roll forward again.
// A manual rollback has no verification clock: the operator chose it.
func (c *Controller) Rollback(actor, id string) (Workspace, error) {
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return Workspace{}, err
	}
	if w.Desired == DesiredDeleted {
		return Workspace{}, ErrConflict
	}
	base, ok := c.profiles[w.Profile]
	if !ok {
		return Workspace{}, fmt.Errorf("%w: unknown profile", ErrInvalid)
	}
	current, _ := c.profileFor(w)
	now := c.now()
	var target, outcome string
	switch {
	case w.Upgrade != nil:
		target, outcome = w.Upgrade.From, UpgradeAborted
		w.LastUpgrade = &UpgradeDone{From: w.Upgrade.From, To: w.Upgrade.To, Outcome: UpgradeAborted, Reason: "rolled back by " + actor, At: now}
		w.Upgrade = nil
	case w.PreviousImage != "":
		target = w.PreviousImage
		w.PreviousImage = current.Image
	default:
		return Workspace{}, fmt.Errorf("%w: no previous image to roll back to", ErrConflict)
	}
	w.Image, w.RolloutPending, w.UpdatedAt = imageOverride(base, target), true, now
	if err := c.store.Put(w); err != nil {
		return Workspace{}, err
	}
	s.resetReady()
	detail := "from=" + current.Image + " to=" + target
	if outcome != "" {
		detail += " (aborted upgrade)"
	}
	c.audit(actor, ActionRollback, id, detail, ResultOK)
	c.queue.Add(id)
	return w, nil
}

// verifyUpgrade runs inside Reconcile with the workspace lock held. It looks at
// what the runtime reports and either commits, rolls back, or keeps waiting.
// It mutates w; the caller persists it with the rest of the reconcile result.
//
// The returned observation is not Ready while the old image is still the one
// serving: the workspace must not report Running on an image it is leaving.
func (c *Controller) verifyUpgrade(w *Workspace, base Profile, obs Observation, now time.Time) Observation {
	u := w.Upgrade
	if u.Deadline.IsZero() {
		u.Deadline = now.Add(u.Timeout)
	}
	if obs.Ready && obs.Image == u.To {
		if u.ReadySince.IsZero() {
			u.ReadySince = now
		}
		if now.Sub(u.ReadySince) >= c.UpgradeSettle {
			w.PreviousImage = u.From
			w.LastUpgrade = &UpgradeDone{From: u.From, To: u.To, Outcome: UpgradeCommitted, At: now}
			w.Upgrade = nil
			c.Metrics.UpgradeCommits.Add(1)
			c.audit(ActorSystem, ActionUpgradeCommit, w.ID, "from="+u.From+" to="+u.To, ResultOK)
		}
		return obs
	}
	u.ReadySince = time.Time{}
	obs.Ready = false
	if now.Before(u.Deadline) {
		return obs
	}
	reason := fmt.Sprintf("not ready on %s within %s", u.To, u.Timeout)
	w.LastUpgrade = &UpgradeDone{From: u.From, To: u.To, Outcome: UpgradeRolledBack, Reason: reason, At: now}
	w.Image, w.RolloutPending = imageOverride(base, u.From), true
	w.Upgrade = nil
	c.Metrics.UpgradeRollbacks.Add(1)
	c.audit(ActorSystem, ActionUpgradeRollback, w.ID, "from="+u.To+" to="+u.From+": "+reason, ResultError)
	return obs
}

// pauseUpgrade stops the verification clock while the workspace is not meant
// to run. Otherwise a stopped workspace would come back from a weekend with
// its deadline already past and be rolled back without ever having tried.
func pauseUpgrade(w *Workspace) {
	if w.Upgrade != nil {
		w.Upgrade.Deadline, w.Upgrade.ReadySince = time.Time{}, time.Time{}
	}
}
