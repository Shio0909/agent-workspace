package agent

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// flakyLLM fails the first `failures` calls with `status`, then answers.
func flakyLLM(t *testing.T, status, failures int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if int(calls.Add(1)) <= failures {
			w.WriteHeader(status)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"done"}}]}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func retryAgent(t *testing.T, url string, retries int) *Agent {
	t.Helper()
	creds := t.TempDir()
	if err := os.WriteFile(filepath.Join(creds, CredentialKey), []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	return New(Config{WorkspaceDir: t.TempDir(), CredentialDir: creds, LLMBaseURL: url + "/v1",
		LLMRetries: retries, RetryBackoff: time.Millisecond})
}

func TestTransientLLMFailuresAreRetried(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable} {
		srv, calls := flakyLLM(t, status, 2)
		reply, _, _, err := retryAgent(t, srv.URL, 2).run(t.Context(), "s", "hi")
		if err != nil || reply != "done" {
			t.Fatalf("status %d: reply %q err %v", status, reply, err)
		}
		if calls.Load() != 3 {
			t.Fatalf("status %d: expected 3 attempts, got %d", status, calls.Load())
		}
	}
}

func TestRetriesAreBounded(t *testing.T) {
	srv, calls := flakyLLM(t, http.StatusBadGateway, 100)
	if _, _, _, err := retryAgent(t, srv.URL, 2).run(t.Context(), "s", "hi"); err == nil {
		t.Fatal("expected an error once retries are exhausted")
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 1 attempt + 2 retries, got %d", calls.Load())
	}
}

func TestRejectedCredentialAndBadRequestsAreNotRetried(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest} {
		srv, calls := flakyLLM(t, status, 100)
		_, _, _, err := retryAgent(t, srv.URL, 2).run(t.Context(), "s", "hi")
		if err == nil {
			t.Fatalf("status %d: expected an error", status)
		}
		if calls.Load() != 1 {
			t.Fatalf("status %d must not be retried, got %d attempts", status, calls.Load())
		}
	}
}

func TestRetriedTurnLeavesOneUserMessageInHistory(t *testing.T) {
	srv, _ := flakyLLM(t, http.StatusBadGateway, 2)
	a := retryAgent(t, srv.URL, 2)
	if _, _, _, err := a.run(t.Context(), "s", "hi"); err != nil {
		t.Fatal(err)
	}
	history, err := a.loadSession("s")
	if err != nil || len(history) != 2 {
		t.Fatalf("expected user+assistant only, got %d messages, err %v", len(history), err)
	}
}
