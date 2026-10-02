package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-workspace/internal/control"
)

// meteredWorkload serves a heartbeat with a settable usage and a /note path
// which counts how often the gateway reached it.
type meteredWorkload struct {
	srv   *httptest.Server
	total atomic.Int64
	notes atomic.Int64
}

func newMeteredWorkload(t *testing.T) *meteredWorkload {
	t.Helper()
	m := &meteredWorkload{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/heartbeat" {
			_ = json.NewEncoder(w).Encode(map[string]any{"busy": false, "version": "v1",
				"usage": control.Usage{PromptTokens: m.total.Load(), TotalTokens: m.total.Load()}})
			return
		}
		m.notes.Add(1)
		_, _ = w.Write([]byte("note"))
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func budgetFixture(t *testing.T) (http.Handler, *control.Controller, *meteredWorkload) {
	t.Helper()
	s, err := control.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	m := newMeteredWorkload(t)
	profiles := map[string]control.Profile{"agent": {HeartbeatPath: "/heartbeat"}}
	c := control.New(s, upstreamRuntime{m.srv.URL}, profiles, time.Hour)
	c.PollInterval = time.Millisecond
	for _, id := range []string{"a1", "team-a-1", "team-b-1"} {
		if _, err := c.Create("tester", id, "agent"); err != nil {
			t.Fatal(err)
		}
	}
	srv := &Server{Controller: c, Token: adminSecret, Tokens: []ScopedToken{
		scoped("alpha", alphaSecret, []string{"team-a-*"}, nil),
	}}
	return srv.Handler(), c, m
}

func workspaceOf(t *testing.T, h http.Handler, id string) control.Workspace {
	t.Helper()
	rec := scopedCall(h, adminSecret, http.MethodGet, "/v1/workspaces/"+id, "")
	var w control.Workspace
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &w) != nil {
		t.Fatalf("get %s: %d %s", id, rec.Code, rec.Body)
	}
	return w
}

func TestExceedingTheBudgetRefusesTheGatewayWith402UntilItIsRaised(t *testing.T) {
	h, c, m := budgetFixture(t)
	if rec := scopedCall(h, adminSecret, http.MethodPost, "/v1/workspaces/a1/start", "", "X-Biz-Id", "start-1"); rec.Code != http.StatusAccepted {
		t.Fatalf("start: %d %s", rec.Code, rec.Body)
	}
	if rec := scopedCall(h, adminSecret, http.MethodGet, "/w/a1/note", ""); rec.Code != http.StatusOK {
		t.Fatalf("a workspace within budget serves traffic: %d", rec.Code)
	}
	rec := scopedCall(h, adminSecret, http.MethodPut, "/v1/workspaces/a1/token-budget", `{"token_budget":400}`)
	if rec.Code != http.StatusOK || workspaceOf(t, h, "a1").TokenBudget != 400 {
		t.Fatalf("set budget: %d %s", rec.Code, rec.Body)
	}

	m.total.Store(500)
	c.Beat(context.Background(), "a1", m.srv.URL)
	w := workspaceOf(t, h, "a1")
	if w.Desired != control.DesiredSuspended || w.SuspendedFor != control.SuspendedForBudget || w.Usage.TotalTokens != 500 ||
		!strings.Contains(w.LastError, "token budget exceeded") {
		t.Fatalf("not suspended for the budget: %+v", w)
	}

	reached := m.notes.Load()
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/w/a1/note"},
		{http.MethodPost, "/v1/workspaces/a1/start"},
		{http.MethodPost, "/v1/workspaces/a1/restart"},
	} {
		rec := scopedCall(h, adminSecret, tc.method, tc.path, "", "X-Biz-Id", "refused-"+strings.ReplaceAll(tc.path, "/", "-"))
		if rec.Code != http.StatusPaymentRequired || !strings.Contains(rec.Body.String(), "token budget exceeded") {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, rec.Code, rec.Body)
		}
	}
	if m.notes.Load() != reached {
		t.Fatal("a refused request reached the workload")
	}

	rec = scopedCall(h, adminSecret, http.MethodPut, "/v1/workspaces/a1/token-budget", `{"token_budget":1000}`)
	if rec.Code != http.StatusOK || workspaceOf(t, h, "a1").SuspendedFor != "" {
		t.Fatalf("raise: %d %s", rec.Code, rec.Body)
	}
	if rec := scopedCall(h, adminSecret, http.MethodGet, "/w/a1/note", ""); rec.Code != http.StatusOK {
		t.Fatalf("after raising the budget the gateway wakes it again: %d %s", rec.Code, rec.Body)
	}
}

func TestTokenBudgetRequestValidation(t *testing.T) {
	h, _, _ := budgetFixture(t)
	for name, tc := range map[string]struct {
		path, body, token string
		want              int
	}{
		"negative":        {"/v1/workspaces/a1/token-budget", `{"token_budget":-1}`, adminSecret, http.StatusBadRequest},
		"absurd":          {"/v1/workspaces/a1/token-budget", `{"token_budget":9007199254740993000}`, adminSecret, http.StatusBadRequest},
		"missing field":   {"/v1/workspaces/a1/token-budget", `{}`, adminSecret, http.StatusBadRequest},
		"unknown field":   {"/v1/workspaces/a1/token-budget", `{"token_budget":5,"budget":9}`, adminSecret, http.StatusBadRequest},
		"string":          {"/v1/workspaces/a1/token-budget", `{"token_budget":"5"}`, adminSecret, http.StatusBadRequest},
		"fraction":        {"/v1/workspaces/a1/token-budget", `{"token_budget":1.5}`, adminSecret, http.StatusBadRequest},
		"empty body":      {"/v1/workspaces/a1/token-budget", ``, adminSecret, http.StatusBadRequest},
		"unknown":         {"/v1/workspaces/nope/token-budget", `{"token_budget":5}`, adminSecret, http.StatusNotFound},
		"no token":        {"/v1/workspaces/a1/token-budget", `{"token_budget":5}`, "", http.StatusUnauthorized},
		"wrong token":     {"/v1/workspaces/a1/token-budget", `{"token_budget":5}`, "wrong", http.StatusUnauthorized},
		"zero is allowed": {"/v1/workspaces/a1/token-budget", `{"token_budget":0}`, adminSecret, http.StatusOK},
	} {
		if rec := scopedCall(h, tc.token, http.MethodPut, tc.path, tc.body); rec.Code != tc.want {
			t.Errorf("%s: got %d want %d (%s)", name, rec.Code, tc.want, strings.TrimSpace(rec.Body.String()))
		}
	}
	if got := workspaceOf(t, h, "a1").TokenBudget; got != 0 {
		t.Fatalf("a rejected request changed the budget: %d", got)
	}
}

func TestTokenBudgetHonoursTheIdempotencyKey(t *testing.T) {
	h, _, _ := budgetFixture(t)
	put := func(id, bizID, body string) *httptest.ResponseRecorder {
		return scopedCall(h, adminSecret, http.MethodPut, "/v1/workspaces/"+id+"/token-budget", body, "X-Biz-Id", bizID)
	}
	if rec := put("a1", "budget-1", `{"token_budget":500}`); rec.Code != http.StatusOK {
		t.Fatalf("first: %d %s", rec.Code, rec.Body)
	}
	// A retry which arrives after someone else raised the budget must not undo it.
	if rec := scopedCall(h, adminSecret, http.MethodPut, "/v1/workspaces/a1/token-budget", `{"token_budget":900}`); rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
	rec := put("a1", "budget-1", `{"token_budget":500}`)
	var op control.Operation
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &op) != nil || op.BizID != "budget-1" || op.Type != control.OpTokenBudget || op.Status != control.OpSuccess {
		t.Fatalf("replay must return the first record: %d %s", rec.Code, rec.Body)
	}
	if got := workspaceOf(t, h, "a1").TokenBudget; got != 900 {
		t.Fatalf("the replay was applied again: %d", got)
	}
	if rec := put("team-a-1", "budget-1", `{"token_budget":1}`); rec.Code != http.StatusConflict {
		t.Fatalf("one key cannot cover two workspaces: %d %s", rec.Code, rec.Body)
	}
	if rec := scopedCall(h, adminSecret, http.MethodPost, "/v1/workspaces/a1/stop", "", "X-Biz-Id", "budget-1"); rec.Code != http.StatusConflict {
		t.Fatalf("one key cannot cover two actions: %d %s", rec.Code, rec.Body)
	}
}

func TestScopedTokenCanSetTheBudgetOnlyWithinItsScope(t *testing.T) {
	h, c, _ := budgetFixture(t)
	if rec := scopedCall(h, alphaSecret, http.MethodPut, "/v1/workspaces/team-a-1/token-budget", `{"token_budget":700}`); rec.Code != http.StatusOK {
		t.Fatalf("inside the scope: %d %s", rec.Code, rec.Body)
	}
	if got := workspaceOf(t, h, "team-a-1").TokenBudget; got != 700 {
		t.Fatalf("%d", got)
	}
	outside := scopedCall(h, alphaSecret, http.MethodPut, "/v1/workspaces/team-b-1/token-budget", `{"token_budget":700}`)
	missing := scopedCall(h, alphaSecret, http.MethodPut, "/v1/workspaces/team-zzz/token-budget", `{"token_budget":700}`)
	if outside.Code != http.StatusNotFound || outside.Body.String() != missing.Body.String() {
		t.Fatalf("outside the scope must look like a missing workspace: %d %q vs %d %q", outside.Code, outside.Body, missing.Code, missing.Body)
	}
	if got := workspaceOf(t, h, "team-b-1").TokenBudget; got != 0 {
		t.Fatalf("a scoped token set a budget outside its scope: %d", got)
	}
	// The same applies to the idempotent form, and a refused call leaves no record behind.
	if rec := scopedCall(h, alphaSecret, http.MethodPut, "/v1/workspaces/team-b-1/token-budget", `{"token_budget":1}`, "X-Biz-Id", "x-1"); rec.Code != http.StatusNotFound {
		t.Fatalf("%d", rec.Code)
	}
	if _, err := c.BeginOperation("x-1", "team-b-1", control.OpTokenBudget); err != nil {
		t.Fatalf("the refused request consumed its idempotency key: %v", err)
	}
	events, err := c.Audit(control.AuditQuery{Workspace: "team-a-1", Action: control.ActionBudgetSet})
	if err != nil || len(events) != 1 || events[0].Actor != ActorPrefix+"alpha" {
		t.Fatalf("the audit trail must name the token: %+v %v", events, err)
	}
}

func TestUsageMetricsAreExposedByProfile(t *testing.T) {
	h, c, m := budgetFixture(t)
	scopedCall(h, adminSecret, http.MethodPost, "/v1/workspaces/a1/start", "", "X-Biz-Id", "start-m")
	m.total.Store(42)
	c.Beat(context.Background(), "a1", m.srv.URL)
	rec := scopedCall(h, adminSecret, http.MethodGet, "/metrics", "")
	if !strings.Contains(rec.Body.String(), `nc_tokens_total{profile="agent",kind="prompt"} 42`) {
		t.Fatalf("%s", rec.Body)
	}
	if rec := scopedCall(h, alphaSecret, http.MethodGet, "/metrics", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("usage must stay admin-only: %d", rec.Code)
	}
}
