package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agent-workspace/internal/control"
)

type credentialRuntime struct {
	upstreamRuntime
	values  map[string]string
	version int
}

func (c *credentialRuntime) SetCredentials(_ context.Context, _ control.Workspace, version int, values map[string]string) error {
	c.values, c.version = values, version
	return nil
}

func (c *credentialRuntime) ClearCredentials(context.Context, control.Workspace) error {
	c.values = nil
	return nil
}

func credentialServer(t *testing.T) (http.Handler, *credentialRuntime) {
	t.Helper()
	s, err := control.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	rt := &credentialRuntime{upstreamRuntime: upstreamRuntime{"http://agent.test"}}
	c := control.New(s, rt, map[string]control.Profile{"agent": {CredentialPath: "/run/creds"}, "plain": {}}, time.Minute)
	for id, profile := range map[string]string{"a1": "agent", "p1": "plain"} {
		if _, err := c.Create("tester", id, profile); err != nil {
			t.Fatal(err)
		}
	}
	return (&Server{Controller: c, Token: "test-control-token"}).Handler(), rt
}

func credCall(h http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("X-Control-Token", token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCredentialEndpointsReturnMetadataNeverValues(t *testing.T) {
	h, rt := credentialServer(t)
	const secret = "sk-secret-do-not-leak"
	rec := credCall(h, http.MethodPut, "/v1/workspaces/a1/credentials", `{"values":{"llm_api_key":"`+secret+`"}}`, "test-control-token")
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), secret) {
		t.Fatalf("put: %d %s", rec.Code, rec.Body)
	}
	var info control.CredentialInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil || info.Version != 1 || len(info.Keys) != 1 {
		t.Fatalf("put body: %+v err=%v", info, err)
	}
	if rt.values["llm_api_key"] != secret || rt.version != 1 {
		t.Fatalf("runtime got %v v%d", rt.values, rt.version)
	}

	rec = credCall(h, http.MethodGet, "/v1/workspaces/a1/credentials", "", "test-control-token")
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), secret) || !strings.Contains(rec.Body.String(), `"llm_api_key"`) {
		t.Fatalf("get: %d %s", rec.Code, rec.Body)
	}
	// The workspace record itself must not carry values either.
	rec = credCall(h, http.MethodGet, "/v1/workspaces/a1", "", "test-control-token")
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatal("workspace response leaks the credential")
	}

	rec = credCall(h, http.MethodDelete, "/v1/workspaces/a1/credentials", "", "test-control-token")
	if rec.Code != http.StatusOK || rt.values != nil {
		t.Fatalf("delete: %d values=%v", rec.Code, rt.values)
	}
}

func TestCredentialEndpointsRequireTheControlToken(t *testing.T) {
	h, _ := credentialServer(t)
	for _, method := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
		for _, token := range []string{"", "wrong"} {
			rec := credCall(h, method, "/v1/workspaces/a1/credentials", `{"values":{"k":"v"}}`, token)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s token=%q: %d", method, token, rec.Code)
			}
		}
	}
}

func TestCredentialRequestValidation(t *testing.T) {
	h, _ := credentialServer(t)
	huge := `{"values":{"k":"` + strings.Repeat("v", maxCredentialBody) + `"}}`
	for name, tc := range map[string]struct {
		path, body string
		want       int
	}{
		"profile without credential_path": {"/v1/workspaces/p1/credentials", `{"values":{"k":"v"}}`, http.StatusBadRequest},
		"unknown workspace":               {"/v1/workspaces/nope/credentials", `{"values":{"k":"v"}}`, http.StatusNotFound},
		"empty body":                      {"/v1/workspaces/a1/credentials", ``, http.StatusBadRequest},
		"unknown field":                   {"/v1/workspaces/a1/credentials", `{"values":{"k":"v"},"x":1}`, http.StatusBadRequest},
		"bad key":                         {"/v1/workspaces/a1/credentials", `{"values":{".version":"v"}}`, http.StatusBadRequest},
		"body too large":                  {"/v1/workspaces/a1/credentials", huge, http.StatusBadRequest},
	} {
		rec := credCall(h, http.MethodPut, tc.path, tc.body, "test-control-token")
		if rec.Code != tc.want {
			t.Errorf("%s: got %d want %d (%s)", name, rec.Code, tc.want, bytes.TrimSpace(rec.Body.Bytes()))
		}
		if strings.Contains(rec.Body.String(), "vvvv") {
			t.Errorf("%s: error body echoes the value", name)
		}
	}
}
