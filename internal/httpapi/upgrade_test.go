package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"agent-workspace/internal/control"
)

func upgradeServer(t *testing.T) http.Handler {
	t.Helper()
	s, err := control.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	profile := control.Profile{Image: "reg/agent:v1", AllowedImages: []string{"reg/agent:v2"}}
	c := control.New(s, upstreamRuntime{"http://agent.test"}, map[string]control.Profile{"agent": profile}, time.Minute)
	if _, err := c.Create("tester", "a1", "agent"); err != nil {
		t.Fatal(err)
	}
	return (&Server{Controller: c, Token: "test-control-token"}).Handler()
}

func TestUpgradeEndpointAcceptsAllowedImageAndReportsTheFallback(t *testing.T) {
	h := upgradeServer(t)
	rec := credCall(h, http.MethodPost, "/v1/workspaces/a1/upgrade", `{"image":"reg/agent:v2","timeout_seconds":60}`, "test-control-token")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("upgrade: %d %s", rec.Code, rec.Body)
	}
	var w control.Workspace
	if err := json.Unmarshal(rec.Body.Bytes(), &w); err != nil || w.Upgrade == nil || w.Upgrade.From != "reg/agent:v1" || w.Upgrade.To != "reg/agent:v2" {
		t.Fatalf("body: %+v err=%v", w.Upgrade, err)
	}
	rec = credCall(h, http.MethodPost, "/v1/workspaces/a1/rollback", ``, "test-control-token")
	if rec.Code != http.StatusAccepted || strings.Contains(rec.Body.String(), `"upgrade":`) {
		t.Fatalf("rollback: %d %s", rec.Code, rec.Body)
	}
}

func TestUpgradeEndpointValidation(t *testing.T) {
	h := upgradeServer(t)
	for name, tc := range map[string]struct {
		path, body string
		want       int
	}{
		"image not allowed":       {"/v1/workspaces/a1/upgrade", `{"image":"evil/agent:v1"}`, http.StatusBadRequest},
		"missing image":           {"/v1/workspaces/a1/upgrade", `{}`, http.StatusBadRequest},
		"unknown field":           {"/v1/workspaces/a1/upgrade", `{"image":"reg/agent:v2","x":1}`, http.StatusBadRequest},
		"negative timeout":        {"/v1/workspaces/a1/upgrade", `{"image":"reg/agent:v2","timeout_seconds":-5}`, http.StatusBadRequest},
		"overflowing timeout":     {"/v1/workspaces/a1/upgrade", `{"image":"reg/agent:v2","timeout_seconds":9223372036}`, http.StatusBadRequest},
		"unknown workspace":       {"/v1/workspaces/nope/upgrade", `{"image":"reg/agent:v2"}`, http.StatusNotFound},
		"rollback with no target": {"/v1/workspaces/a1/rollback", ``, http.StatusConflict},
	} {
		rec := credCall(h, http.MethodPost, tc.path, tc.body, "test-control-token")
		if rec.Code != tc.want {
			t.Errorf("%s: got %d want %d (%s)", name, rec.Code, tc.want, strings.TrimSpace(rec.Body.String()))
		}
	}
}

func TestUpgradeEndpointsRequireTheControlToken(t *testing.T) {
	h := upgradeServer(t)
	for _, path := range []string{"/v1/workspaces/a1/upgrade", "/v1/workspaces/a1/rollback"} {
		for _, token := range []string{"", "wrong"} {
			if rec := credCall(h, http.MethodPost, path, `{"image":"reg/agent:v2"}`, token); rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s token=%q: %d", path, token, rec.Code)
			}
		}
	}
}
