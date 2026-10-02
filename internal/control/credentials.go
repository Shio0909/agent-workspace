package control

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	maxCredentialKeys       = 16
	maxCredentialValueBytes = 16 << 10
	maxCredentialTotalBytes = 64 << 10
)

// 键名会成为挂载目录下的文件名。不允许以点开头：".version" 保留给控制面，
// agent 用它判断自己读到的是哪一版凭据。
var credentialKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,62}$`)

// CredentialInfo 是凭据的元数据。它不包含任何值，所以可以安全地返回给调用方。
type CredentialInfo struct {
	Version   int       `json:"credential_version"`
	Keys      []string  `json:"keys"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

func credentialInfo(w Workspace) CredentialInfo {
	keys := w.CredentialKeys
	if keys == nil {
		keys = []string{}
	}
	return CredentialInfo{Version: w.CredentialVersion, Keys: keys, UpdatedAt: w.CredentialUpdatedAt}
}

// ValidateCredentials 检查键名和大小。错误信息只会提到键名，绝不回显值。
func ValidateCredentials(values map[string]string) error {
	if len(values) == 0 || len(values) > maxCredentialKeys {
		return fmt.Errorf("%w: credentials need 1 to %d keys", ErrInvalid, maxCredentialKeys)
	}
	total := 0
	for k, v := range values {
		if !credentialKeyPattern.MatchString(k) {
			return fmt.Errorf("%w: invalid credential key %q", ErrInvalid, k)
		}
		if v == "" || len(v) > maxCredentialValueBytes {
			return fmt.Errorf("%w: credential %q must be 1 to %d bytes", ErrInvalid, k, maxCredentialValueBytes)
		}
		total += len(v)
	}
	if total > maxCredentialTotalBytes {
		return fmt.Errorf("%w: credentials exceed %d bytes in total", ErrInvalid, maxCredentialTotalBytes)
	}
	return nil
}

// credentialTarget returns the runtime capability once the workspace's profile
// has opted in by setting credential_path.
func (c *Controller) credentialTarget(w Workspace) (CredentialStore, error) {
	if p, ok := c.profiles[w.Profile]; !ok || p.CredentialPath == "" {
		return nil, fmt.Errorf("%w: profile has no credential_path", ErrInvalid)
	}
	store, ok := c.runtime.(CredentialStore)
	if !ok {
		return nil, fmt.Errorf("%w: runtime does not store credentials", ErrInvalid)
	}
	return store, nil
}

// SetCredentials replaces the workspace's whole credential set and returns the
// new version. The values go straight to the runtime and are never persisted
// by the controller.
//
// The order is runtime first, store second. A crash in between leaves the
// Secret one version ahead of the store; the next call computes the same next
// version and overwrites it, so the gap heals without a repair step. The
// reverse order would record a version no Secret carries.
//
// Rotation is allowed while the workspace is stopped or suspended, because a
// leaked key must be replaceable even when nothing is running. The pod picks
// the new files up when it starts. Only a deleted workspace refuses.
func (c *Controller) SetCredentials(actor, id string, values map[string]string) (CredentialInfo, error) {
	if err := ValidateCredentials(values); err != nil {
		return CredentialInfo{}, err
	}
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return CredentialInfo{}, err
	}
	if w.Desired == DesiredDeleted {
		return CredentialInfo{}, ErrConflict
	}
	target, err := c.credentialTarget(w)
	if err != nil {
		return CredentialInfo{}, err
	}
	version := w.CredentialVersion + 1
	ctx, cancel := context.WithTimeout(context.Background(), c.OperationTimeout)
	defer cancel()
	if err := target.SetCredentials(ctx, w, version, values); err != nil {
		c.audit(actor, ActionCredentialSet, id, "failed to store credentials", ResultError)
		return CredentialInfo{}, err
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	now := c.now()
	w.CredentialVersion, w.CredentialKeys, w.CredentialUpdatedAt, w.UpdatedAt = version, keys, now, now
	if err := c.store.Put(w); err != nil {
		return CredentialInfo{}, err
	}
	c.audit(actor, ActionCredentialSet, id, fmt.Sprintf("version=%d keys=%s", version, strings.Join(keys, ",")), ResultOK)
	c.restartForCredentials(actor, id, s)
	return credentialInfo(w), nil
}

// ClearCredentials removes the workspace's credentials and bumps the version,
// so an agent that reports its loaded version can tell a clear from a no-op.
func (c *Controller) ClearCredentials(actor, id string) (CredentialInfo, error) {
	s := c.slot(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := c.store.Get(id)
	if err != nil {
		return CredentialInfo{}, err
	}
	if w.Desired == DesiredDeleted {
		return CredentialInfo{}, ErrConflict
	}
	target, err := c.credentialTarget(w)
	if err != nil {
		return CredentialInfo{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.OperationTimeout)
	defer cancel()
	if err := target.ClearCredentials(ctx, w); err != nil {
		c.audit(actor, ActionCredentialClear, id, "failed to clear credentials", ResultError)
		return CredentialInfo{}, err
	}
	now := c.now()
	w.CredentialVersion++
	w.CredentialKeys, w.CredentialUpdatedAt, w.UpdatedAt = nil, now, now
	if err := c.store.Put(w); err != nil {
		return CredentialInfo{}, err
	}
	c.audit(actor, ActionCredentialClear, id, fmt.Sprintf("version=%d", w.CredentialVersion), ResultOK)
	c.restartForCredentials(actor, id, s)
	return credentialInfo(w), nil
}

// Credentials returns the metadata of the workspace's credentials.
func (c *Controller) Credentials(id string) (CredentialInfo, error) {
	w, err := c.store.Get(id)
	if err != nil {
		return CredentialInfo{}, err
	}
	return credentialInfo(w), nil
}

// restartForCredentials asks for a workload replacement when the profile
// cannot reload credentials by itself. The caller holds the slot lock. A
// failure here is not a failure of the credential change, which is already
// durable: the error is audited and the next restart or start picks the key up.
func (c *Controller) restartForCredentials(actor, id string, s *slot) {
	w, err := c.store.Get(id)
	if err != nil || w.Desired != DesiredRunning || !c.profiles[w.Profile].RestartOnCredentialChange {
		return
	}
	w.RestartPending, w.UpdatedAt = true, c.now()
	if err := c.store.Put(w); err != nil {
		c.audit(actor, ActionRestart, id, "restart after credential change not recorded", ResultError)
		return
	}
	s.resetReady()
	c.queue.Add(id)
	c.audit(actor, ActionRestart, id, "workload replaced to load the new credentials", ResultOK)
}
