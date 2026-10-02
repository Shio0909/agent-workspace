package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type credRuntime struct {
	fakeRuntime
	versions  []int
	values    map[string]string
	clears    int
	failSet   bool
	failClear bool
}

func (r *credRuntime) SetCredentials(_ context.Context, _ Workspace, version int, values map[string]string) error {
	if r.failSet {
		return errors.New("secret write failed")
	}
	r.versions = append(r.versions, version)
	r.values = values
	return nil
}

func (r *credRuntime) ClearCredentials(context.Context, Workspace) error {
	if r.failClear {
		return errors.New("secret delete failed")
	}
	r.clears++
	r.values = nil
	return nil
}

func credFixture(t *testing.T, profile Profile) (*Controller, *credRuntime) {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := &credRuntime{}
	c := New(s, r, map[string]Profile{"agent": profile}, time.Minute)
	if _, err := c.Create(testActor, "a1", "agent"); err != nil {
		t.Fatal(err)
	}
	return c, r
}

func TestCredentialsVersionAndKeysButNeverValues(t *testing.T) {
	c, r := credFixture(t, Profile{CredentialPath: "/run/creds"})
	const secret = "sk-very-secret-value"
	info, err := c.SetCredentials(testActor, "a1", map[string]string{"llm_api_key": secret, "other": "x"})
	if err != nil || info.Version != 1 || strings.Join(info.Keys, ",") != "llm_api_key,other" {
		t.Fatalf("info=%+v err=%v", info, err)
	}
	if info, err = c.SetCredentials(testActor, "a1", map[string]string{"llm_api_key": secret + "2"}); err != nil || info.Version != 2 {
		t.Fatalf("rotation: info=%+v err=%v", info, err)
	}
	if len(r.versions) != 2 || r.versions[1] != 2 {
		t.Fatalf("runtime saw versions %v", r.versions)
	}

	// The value must not appear in anything the controller persists: not in
	// the workspace record and not in the audit log.
	w, _ := c.Get("a1")
	if strings.Contains(strings.Join(w.CredentialKeys, " "), secret) {
		t.Fatal("workspace record holds a value")
	}
	events, err := c.Audit(AuditQuery{Workspace: "a1", Action: ActionCredentialSet})
	if err != nil || len(events) != 2 {
		t.Fatalf("audit events=%v err=%v", events, err)
	}
	for _, e := range events {
		if strings.Contains(e.Detail, "sk-") || !strings.Contains(e.Detail, "version=") {
			t.Fatalf("audit detail %q", e.Detail)
		}
	}
}

func TestCredentialsSurviveControllerRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := &credRuntime{}
	profiles := map[string]Profile{"agent": {CredentialPath: "/run/creds"}}
	c := New(s, r, profiles, time.Minute)
	if _, err := c.Create(testActor, "a1", "agent"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"llm_api_key": "k"}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c = New(s, r, profiles, time.Minute)
	// The next rotation continues from the persisted version instead of
	// restarting at 1, so an agent never sees the version go backwards.
	info, err := c.SetCredentials(testActor, "a1", map[string]string{"llm_api_key": "k2"})
	if err != nil || info.Version != 2 {
		t.Fatalf("info=%+v err=%v", info, err)
	}
}

func TestCredentialsRequireProfileOptInAndRuntimeSupport(t *testing.T) {
	c, _ := credFixture(t, Profile{})
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"k": "v"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("profile without credential_path: %v", err)
	}
	// fakeRuntime does not implement CredentialStore.
	c2, _ := fixture(t)
	c2.profiles["demo"] = Profile{CredentialPath: "/run/creds"}
	if _, err := c2.SetCredentials(testActor, "demo", map[string]string{"k": "v"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("runtime without capability: %v", err)
	}
}

func TestCredentialValidationNeverEchoesValues(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"empty":        {},
		"dot key":      {".version": "x"},
		"slash key":    {"a/b": "x"},
		"empty value":  {"k": ""},
		"huge value":   {"k": strings.Repeat("v", maxCredentialValueBytes+1)},
		"too many":     manyKeys(maxCredentialKeys + 1),
		"over total":   overTotal(),
		"long key":     {strings.Repeat("k", 64): "x"},
		"space in key": {"a b": "x"},
	} {
		err := ValidateCredentials(values)
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err=%v", name, err)
		}
		if err != nil && strings.Contains(err.Error(), "vvvv") {
			t.Errorf("%s: error echoes a value", name)
		}
	}
	if err := ValidateCredentials(map[string]string{"llm_api_key": "x", "A-b.c_1": "y"}); err != nil {
		t.Fatal(err)
	}
}

func manyKeys(n int) map[string]string {
	m := map[string]string{}
	for i := 0; i < n; i++ {
		m["k"+string(rune('a'+i))] = "v"
	}
	return m
}

func overTotal() map[string]string {
	m := map[string]string{}
	for i := 0; i < 5; i++ {
		m["k"+string(rune('a'+i))] = strings.Repeat("v", maxCredentialValueBytes)
	}
	return m
}

func TestCredentialRuntimeFailureLeavesVersionUntouched(t *testing.T) {
	c, r := credFixture(t, Profile{CredentialPath: "/run/creds"})
	r.failSet = true
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"k": "v"}); err == nil {
		t.Fatal("expected failure")
	}
	if info, _ := c.Credentials("a1"); info.Version != 0 {
		t.Fatalf("version advanced to %d without a stored secret", info.Version)
	}
	events, _ := c.Audit(AuditQuery{Workspace: "a1", Action: ActionCredentialSet})
	if len(events) != 1 || events[0].Result != ResultError {
		t.Fatalf("failure not audited: %v", events)
	}
}

func TestCredentialClearBumpsVersion(t *testing.T) {
	c, r := credFixture(t, Profile{CredentialPath: "/run/creds"})
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	info, err := c.ClearCredentials(testActor, "a1")
	if err != nil || info.Version != 2 || len(info.Keys) != 0 || r.clears != 1 {
		t.Fatalf("info=%+v err=%v clears=%d", info, err, r.clears)
	}
	r.failClear = true
	if _, err := c.ClearCredentials(testActor, "a1"); err == nil {
		t.Fatal("expected failure")
	}
}

func TestCredentialsAllowedWhileStoppedRefusedWhenDeleted(t *testing.T) {
	c, _ := credFixture(t, Profile{CredentialPath: "/run/creds"})
	// Created workspaces are stopped: rotating a leaked key must not need a
	// running pod.
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetDesired(testActor, "a1", DesiredDeleted); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"k": "v"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("deleted workspace accepted credentials: %v", err)
	}
	if _, err := c.SetCredentials(testActor, "missing", map[string]string{"k": "v"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing workspace: %v", err)
	}
}

func TestProfileValidationCredentialPath(t *testing.T) {
	base := Profile{Image: "i", Port: 8080, HealthPath: "/h", MountPath: "/workspace", Storage: "1Gi", CPU: "1", Memory: "1Gi"}
	for path, ok := range map[string]bool{"": true, "/run/creds": true, "relative": false, "/": false, "/workspace": false} {
		p := base
		p.CredentialPath = path
		if err := p.Validate(); (err == nil) != ok {
			t.Errorf("credential_path %q: err=%v", path, err)
		}
	}
}

func TestRestartOnCredentialChangeReplacesRunningWorkload(t *testing.T) {
	c, r := credFixture(t, Profile{CredentialPath: "/run/creds", RestartOnCredentialChange: true})
	ctx := context.Background()
	if _, err := c.SetDesired(testActor, "a1", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, restarts := r.counts(); restarts != 0 {
		t.Fatalf("restart before any credential change: %d", restarts)
	}
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"llm_api_key": "k2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, restarts := r.counts(); restarts != 1 {
		t.Fatalf("rotation did not replace the workload: restarts=%d", restarts)
	}
	if _, err := c.ClearCredentials(testActor, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, restarts := r.counts(); restarts != 2 {
		t.Fatalf("clear did not replace the workload: restarts=%d", restarts)
	}
}

func TestRotationDoesNotRestartWithoutOptInOrWhenNotRunning(t *testing.T) {
	ctx := context.Background()
	c, r := credFixture(t, Profile{CredentialPath: "/run/creds"})
	if _, err := c.SetDesired(testActor, "a1", DesiredRunning); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"llm_api_key": "k"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, restarts := r.counts(); restarts != 0 {
		t.Fatalf("profile without opt-in was restarted: %d", restarts)
	}

	c, r = credFixture(t, Profile{CredentialPath: "/run/creds", RestartOnCredentialChange: true})
	if _, err := c.SetCredentials(testActor, "a1", map[string]string{"llm_api_key": "k"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reconcile(ctx, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, restarts := r.counts(); restarts != 0 {
		t.Fatalf("a stopped workspace was restarted: %d", restarts)
	}
}
