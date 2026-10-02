package httpapi

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"agent-workspace/internal/control"
)

const (
	adminSecret  = "test-control-token"
	alphaSecret  = "alpha-secret-token"
	agentsSecret = "agents-secret-token"
	bothSecret   = "both-secret-token"
)

func scoped(name, secret string, workspaces, profiles []string) ScopedToken {
	return ScopedToken{Name: name, Digest: TokenDigest(secret), Workspaces: workspaces, Profiles: profiles}
}

// scopedFixture has three workspaces: team-a-1 (agent), team-a-2 (plain) and
// team-b-1 (agent). alpha may use the team-a-* ids, agents may use the agent
// profile, and both may use only what satisfies both.
func scopedFixture(t *testing.T) (http.Handler, *control.Controller, *credentialRuntime) {
	t.Helper()
	s, err := control.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	rt := &credentialRuntime{upstreamRuntime: upstreamRuntime{"http://agent.test"}}
	c := control.New(s, rt, map[string]control.Profile{"agent": {CredentialPath: "/run/creds"}, "plain": {}}, time.Minute)
	for id, profile := range map[string]string{"team-a-1": "agent", "team-a-2": "plain", "team-b-1": "agent"} {
		if _, err := c.Create("tester", id, profile); err != nil {
			t.Fatal(err)
		}
	}
	srv := &Server{Controller: c, Token: adminSecret, Tokens: []ScopedToken{
		scoped("alpha", alphaSecret, []string{"team-a-*"}, nil),
		scoped("agents", agentsSecret, nil, []string{"agent"}),
		scoped("both", bothSecret, []string{"team-a-*"}, []string{"agent"}),
	}}
	return srv.Handler(), c, rt
}

func scopedCall(h http.Handler, token, method, path, body string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("X-Control-Token", token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func listedIDs(t *testing.T, h http.Handler, token string) []string {
	t.Helper()
	rec := scopedCall(h, token, http.MethodGet, "/v1/workspaces", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}
	var items []control.Workspace
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func TestScopedTokenReachesOnlyItsWorkspaces(t *testing.T) {
	h, c, rt := scopedFixture(t)

	for _, id := range []string{"team-a-1", "team-a-2"} {
		if rec := scopedCall(h, alphaSecret, http.MethodGet, "/v1/workspaces/"+id, ""); rec.Code != http.StatusOK {
			t.Fatalf("alpha on its own workspace %s: %d", id, rec.Code)
		}
	}
	rec := scopedCall(h, alphaSecret, http.MethodPut, "/v1/workspaces/team-a-1/credentials", `{"values":{"llm_api_key":"k"}}`)
	if rec.Code != http.StatusOK || rt.values["llm_api_key"] != "k" {
		t.Fatalf("alpha could not rotate its own credentials: %d %s", rec.Code, rec.Body)
	}
	rt.values = nil

	// Every route that names a workspace must refuse a workspace outside the
	// scope, and the refusal must look exactly like a missing workspace.
	missing := scopedCall(h, adminSecret, http.MethodGet, "/v1/workspaces/team-z-1", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("control: missing workspace: %d", missing.Code)
	}
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/v1/workspaces/team-b-1", ""},
		{"PUT", "/v1/workspaces/team-b-1/credentials", `{"values":{"llm_api_key":"stolen"}}`},
		{"GET", "/v1/workspaces/team-b-1/credentials", ""},
		{"DELETE", "/v1/workspaces/team-b-1/credentials", ""},
		{"POST", "/v1/workspaces/team-b-1/start", ""},
		{"POST", "/v1/workspaces/team-b-1/stop", ""},
		{"POST", "/v1/workspaces/team-b-1/restart", ""},
		{"DELETE", "/v1/workspaces/team-b-1", ""},
		{"POST", "/v1/workspaces/team-b-1/expiry", `{"expires_in_seconds":60}`},
		{"POST", "/v1/workspaces/team-b-1/upgrade", `{"image":"reg/agent:v2"}`},
		{"POST", "/v1/workspaces/team-b-1/rollback", ""},
		{"POST", "/v1/workspaces/team-b-1/leases", `{"ttl_seconds":60}`},
		{"PUT", "/v1/workspaces/team-b-1/leases/x", `{"ttl_seconds":60}`},
		{"DELETE", "/v1/workspaces/team-b-1/leases/x", ""},
		{"POST", "/w/team-b-1/api/v1/chat", `{}`},
		{"GET", "/v1/workspaces/team-z-1", ""},
	} {
		got := scopedCall(h, alphaSecret, tc.method, tc.path, tc.body, "X-Biz-Id", "biz-1")
		if got.Code != http.StatusNotFound || got.Body.String() != missing.Body.String() {
			t.Fatalf("%s %s: %d %q, want the same 404 as a missing workspace %q", tc.method, tc.path, got.Code, got.Body, missing.Body)
		}
	}

	// The refused calls had no effect.
	if rt.values != nil {
		t.Fatalf("credentials were written to a workspace outside the scope: %v", rt.values)
	}
	other, err := c.Get("team-b-1")
	if err != nil || other.Desired != control.DesiredStopped || other.CredentialVersion != 0 {
		t.Fatalf("team-b-1 was changed: %+v %v", other, err)
	}
	if rec := scopedCall(h, adminSecret, http.MethodGet, "/v1/workspaces/team-b-1", ""); rec.Code != http.StatusOK {
		t.Fatalf("the shared token lost access: %d", rec.Code)
	}
}

func TestProfileScopedToken(t *testing.T) {
	h, _, _ := scopedFixture(t)
	for id, want := range map[string]int{
		"team-a-1": http.StatusOK,
		"team-b-1": http.StatusOK,
		"team-a-2": http.StatusNotFound, // plain profile
		"team-z-1": http.StatusNotFound, // does not exist
	} {
		if rec := scopedCall(h, agentsSecret, http.MethodGet, "/v1/workspaces/"+id, ""); rec.Code != want {
			t.Fatalf("agents on %s: %d, want %d", id, rec.Code, want)
		}
	}
	denied := scopedCall(h, agentsSecret, http.MethodGet, "/v1/workspaces/team-a-2", "")
	missing := scopedCall(h, agentsSecret, http.MethodGet, "/v1/workspaces/team-z-1", "")
	if denied.Body.String() != missing.Body.String() {
		t.Fatalf("a workspace of another profile is distinguishable from a missing one: %q vs %q", denied.Body, missing.Body)
	}
}

func TestScopeIsTheIntersectionOfIDsAndProfiles(t *testing.T) {
	h, _, _ := scopedFixture(t)
	for id, want := range map[string]int{
		"team-a-1": http.StatusOK,       // prefix and profile match
		"team-b-1": http.StatusNotFound, // right profile, wrong prefix
		"team-a-2": http.StatusNotFound, // right prefix, wrong profile
	} {
		if rec := scopedCall(h, bothSecret, http.MethodGet, "/v1/workspaces/"+id, ""); rec.Code != want {
			t.Fatalf("both on %s: %d, want %d", id, rec.Code, want)
		}
	}
}

func TestScopedListShowsOnlyWhatTheTokenMayUse(t *testing.T) {
	h, _, _ := scopedFixture(t)
	for token, want := range map[string][]string{
		adminSecret:  {"team-a-1", "team-a-2", "team-b-1"},
		alphaSecret:  {"team-a-1", "team-a-2"},
		agentsSecret: {"team-a-1", "team-b-1"},
		bothSecret:   {"team-a-1"},
	} {
		if got := listedIDs(t, h, token); !slices.Equal(got, want) {
			t.Fatalf("token %s lists %v, want %v", token, got, want)
		}
	}
}

func TestScopedCreateStaysInsideTheScope(t *testing.T) {
	h, _, _ := scopedFixture(t)
	for _, tc := range []struct {
		token, id, profile string
		want               int
	}{
		{alphaSecret, "team-a-3", "agent", http.StatusCreated},
		{alphaSecret, "team-b-9", "agent", http.StatusForbidden},
		{bothSecret, "team-a-4", "agent", http.StatusCreated},
		{bothSecret, "team-a-5", "plain", http.StatusForbidden},
		{agentsSecret, "solo-1", "agent", http.StatusCreated},
		{agentsSecret, "solo-2", "plain", http.StatusForbidden},
		{adminSecret, "anything", "plain", http.StatusCreated},
	} {
		rec := scopedCall(h, tc.token, http.MethodPost, "/v1/workspaces", `{"id":"`+tc.id+`","profile":"`+tc.profile+`"}`)
		if rec.Code != tc.want {
			t.Fatalf("create %s/%s with %s: %d, want %d (%s)", tc.id, tc.profile, tc.token, rec.Code, tc.want, rec.Body)
		}
	}
	if slices.Contains(listedIDs(t, h, adminSecret), "team-b-9") || slices.Contains(listedIDs(t, h, adminSecret), "solo-2") {
		t.Fatal("a refused create left a workspace behind")
	}
}

func TestScopedAuditNeedsAWorkspaceTheTokenMayUse(t *testing.T) {
	h, _, _ := scopedFixture(t)
	for path, want := range map[string]int{
		"/v1/audit":                      http.StatusForbidden,
		"/v1/audit?action=create":        http.StatusForbidden,
		"/v1/audit?workspace=team-b-1":   http.StatusNotFound,
		"/v1/audit?workspace=team-z-1":   http.StatusNotFound,
		"/v1/audit?workspace=team-a-1":   http.StatusOK,
		"/v1/audit?workspace=team-a-2":   http.StatusOK,
		"/v1/audit?limit=nope":           http.StatusBadRequest,
		"/v1/audit?workspace=team-a-1&x": http.StatusOK,
	} {
		if rec := scopedCall(h, alphaSecret, http.MethodGet, path, ""); rec.Code != want {
			t.Fatalf("alpha GET %s: %d, want %d", path, rec.Code, want)
		}
	}
	if rec := scopedCall(h, adminSecret, http.MethodGet, "/v1/audit", ""); rec.Code != http.StatusOK {
		t.Fatalf("admin audit: %d", rec.Code)
	}
}

func TestScopedTokenCannotChooseItsAuditActor(t *testing.T) {
	h, c, _ := scopedFixture(t)
	body := `{"values":{"llm_api_key":"k"}}`
	if rec := scopedCall(h, alphaSecret, http.MethodPut, "/v1/workspaces/team-a-1/credentials", body, "X-Actor", "root"); rec.Code != http.StatusOK {
		t.Fatalf("rotate: %d", rec.Code)
	}
	// The shared token may name any caller, but not claim to be a scoped token.
	if rec := scopedCall(h, adminSecret, http.MethodPut, "/v1/workspaces/team-a-1/credentials", body, "X-Actor", "token:alpha"); rec.Code != http.StatusOK {
		t.Fatalf("rotate as admin: %d", rec.Code)
	}
	if rec := scopedCall(h, adminSecret, http.MethodPut, "/v1/workspaces/team-a-1/credentials", body, "X-Actor", "deploy-bot"); rec.Code != http.StatusOK {
		t.Fatalf("rotate as admin: %d", rec.Code)
	}
	events, err := c.Audit(control.AuditQuery{Workspace: "team-a-1"})
	if err != nil {
		t.Fatal(err)
	}
	var actors []string
	for _, e := range events {
		if e.Actor != "tester" {
			actors = append(actors, e.Actor)
		}
	}
	want := []string{"token:alpha", control.ActorUnknown, "deploy-bot"}
	if !slices.Equal(actors, want) {
		t.Fatalf("audit actors %v, want %v", actors, want)
	}
}

func TestScopedTokenCannotReadAdminEndpoints(t *testing.T) {
	h, _, _ := scopedFixture(t)
	for _, token := range []string{alphaSecret, agentsSecret, bothSecret} {
		if rec := scopedCall(h, token, http.MethodGet, "/metrics", ""); rec.Code != http.StatusForbidden {
			t.Fatalf("scoped token read /metrics: %d", rec.Code)
		}
	}
	if rec := scopedCall(h, adminSecret, http.MethodGet, "/metrics", ""); rec.Code != http.StatusOK {
		t.Fatalf("admin /metrics: %d", rec.Code)
	}
	// Probes stay open for the orchestrator.
	if rec := scopedCall(h, "", http.MethodGet, "/health", ""); rec.Code != http.StatusOK {
		t.Fatalf("/health: %d", rec.Code)
	}
}

func TestOnlyTheTokenItselfAuthenticates(t *testing.T) {
	h, _, _ := scopedFixture(t)
	digest := TokenDigest(alphaSecret)
	for _, token := range []string{"", "wrong", hex.EncodeToString(digest[:]), alphaSecret + " ", strings.ToUpper(alphaSecret)} {
		if rec := scopedCall(h, token, http.MethodGet, "/v1/workspaces", ""); rec.Code != http.StatusUnauthorized {
			t.Fatalf("token %q: %d", token, rec.Code)
		}
	}
}

func TestScopedTokensAloneAreEnoughWithoutASharedToken(t *testing.T) {
	s, err := control.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := control.New(s, upstreamRuntime{"http://agent.test"}, map[string]control.Profile{"plain": {}}, time.Minute)
	h := (&Server{Controller: c, Tokens: []ScopedToken{scoped("alpha", alphaSecret, []string{"a-*"}, nil)}}).Handler()
	if rec := scopedCall(h, alphaSecret, http.MethodGet, "/v1/workspaces", ""); rec.Code != http.StatusOK {
		t.Fatalf("scoped token: %d", rec.Code)
	}
	if rec := scopedCall(h, "", http.MethodGet, "/v1/workspaces", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("an empty token must not match an unset shared token: %d", rec.Code)
	}
}

// A route added later without thinking about authorization must be refused to
// scoped tokens, not open to them.
func TestRoutesDefaultToTheirRestriction(t *testing.T) {
	s := &Server{Token: adminSecret}
	rt := router{ServeMux: http.NewServeMux(), s: s}
	ok := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }
	rt.HandleFunc("GET /x", ok)
	rt.HandleFunc("GET /x/{id}", ok)
	rt.open("GET /probe", ok)

	alpha := &principal{token: &ScopedToken{Name: "alpha", Workspaces: []string{"a-*"}}}
	for _, tc := range []struct {
		name, path string
		who        *principal
		want       int
	}{
		{"no-id route, scoped", "/x", alpha, http.StatusForbidden},
		{"no-id route, admin", "/x", &principal{}, http.StatusNoContent},
		{"no-id route, unauthenticated", "/x", nil, http.StatusForbidden},
		{"id route, in scope", "/x/a-1", alpha, http.StatusNoContent},
		{"id route, out of scope", "/x/b-1", alpha, http.StatusNotFound},
		{"id route, unauthenticated", "/x/a-1", nil, http.StatusNotFound},
		{"open route, unauthenticated", "/probe", nil, http.StatusNoContent},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.who != nil {
			req = withPrincipal(req, tc.who)
		}
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("%s: %d, want %d", tc.name, rec.Code, tc.want)
		}
	}
}

func TestScopedTokenPatterns(t *testing.T) {
	tok := &ScopedToken{Workspaces: []string{"exact", "team-*"}}
	for id, want := range map[string]bool{
		"exact": true, "exact2": false, "exac": false,
		"team-": true, "team-a": true, "teams": false, "": false,
	} {
		if got := tok.allowsID(id); got != want {
			t.Fatalf("allowsID(%q) = %v, want %v", id, got, want)
		}
	}
	if !(&ScopedToken{Profiles: []string{"agent"}}).allowsID("anything") {
		t.Fatal("a profile-only token must not restrict ids")
	}
	if (&ScopedToken{Workspaces: []string{"a"}}).allowsProfile("agent") != true {
		t.Fatal("an id-only token must not restrict profiles")
	}
}

func writeTokens(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadScopedTokens(t *testing.T) {
	profiles := map[string]control.Profile{"agent": {}, "plain": {}}
	a, b := TokenDigest("one"), TokenDigest("two")
	ha, hb := hex.EncodeToString(a[:]), hex.EncodeToString(b[:])

	tokens, err := LoadScopedTokens(writeTokens(t, `[
		{"name":"team-a.ci","sha256":"`+ha+`","workspaces":["team-a-*","solo"]},
		{"name":"agents","sha256":"`+strings.ToUpper(hb)+`","profiles":["agent"]}]`), profiles)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0].Name != "team-a.ci" || tokens[0].Digest != a ||
		!slices.Equal(tokens[0].Workspaces, []string{"team-a-*", "solo"}) || tokens[1].Digest != b {
		t.Fatalf("unexpected tokens: %+v", tokens)
	}

	for name, body := range map[string]string{
		"typo in a key":      `[{"name":"x","sha256":"` + ha + `","workspace":["a"]}]`,
		"no scope":           `[{"name":"x","sha256":"` + ha + `"}]`,
		"empty scope lists":  `[{"name":"x","sha256":"` + ha + `","workspaces":[],"profiles":[]}]`,
		"bare star":          `[{"name":"x","sha256":"` + ha + `","workspaces":["*"]}]`,
		"star in the middle": `[{"name":"x","sha256":"` + ha + `","workspaces":["a*b"]}]`,
		"uppercase id":       `[{"name":"x","sha256":"` + ha + `","workspaces":["Team"]}]`,
		"unknown profile":    `[{"name":"x","sha256":"` + ha + `","profiles":["ghost"]}]`,
		"short digest":       `[{"name":"x","sha256":"abcd","workspaces":["a"]}]`,
		"not hex":            `[{"name":"x","sha256":"` + strings.Repeat("z", 64) + `","workspaces":["a"]}]`,
		"the token itself":   `[{"name":"x","sha256":"one","workspaces":["a"]}]`,
		"bad name":           `[{"name":"has space","sha256":"` + ha + `","workspaces":["a"]}]`,
		"empty name":         `[{"name":"","sha256":"` + ha + `","workspaces":["a"]}]`,
		"duplicate name": `[{"name":"x","sha256":"` + ha + `","workspaces":["a"]},` +
			`{"name":"x","sha256":"` + hb + `","workspaces":["b"]}]`,
		"duplicate token": `[{"name":"x","sha256":"` + ha + `","workspaces":["a"]},` +
			`{"name":"y","sha256":"` + ha + `","workspaces":["b"]}]`,
		"trailing data": `[{"name":"x","sha256":"` + ha + `","workspaces":["a"]}] []`,
		"empty list":    `[]`,
		"not a list":    `{}`,
		"broken json":   `[`,
	} {
		if _, err := LoadScopedTokens(writeTokens(t, body), profiles); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
	if _, err := LoadScopedTokens(filepath.Join(t.TempDir(), "missing.json"), profiles); err == nil {
		t.Fatal("a missing file was accepted")
	}
}

func TestShadowsToken(t *testing.T) {
	tokens := []ScopedToken{scoped("alpha", alphaSecret, []string{"a"}, nil), scoped("oops", adminSecret, []string{"a"}, nil)}
	if name, shadowed := ShadowsToken(tokens, adminSecret); !shadowed || name != "oops" {
		t.Fatalf("shadowing = %q %v", name, shadowed)
	}
	if _, shadowed := ShadowsToken(tokens[:1], adminSecret); shadowed {
		t.Fatal("unrelated token reported as shadowing")
	}
}
