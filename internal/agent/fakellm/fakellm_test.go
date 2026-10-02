package fakellm

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func ask(t *testing.T, h http.Handler, key, message string) (int, string) {
	t.Helper()
	body := `{"messages":[{"role":"user","content":` + quote(message) + `}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"` }

func TestKeysAreEnforced(t *testing.T) {
	h := New("good").Handler()
	if code, _ := ask(t, h, "bad", "hi"); code != http.StatusUnauthorized {
		t.Fatalf("bad key: got %d", code)
	}
	if code, body := ask(t, h, "good", "hi"); code != http.StatusOK || !strings.Contains(body, "echo(1): hi") {
		t.Fatalf("good key: %d %s", code, body)
	}
}

func TestSleepPrefixDelaysAndIsStripped(t *testing.T) {
	h := New("k").Handler()
	start := time.Now()
	code, body := ask(t, h, "k", "sleep 150 slowly")
	if code != http.StatusOK || !strings.Contains(body, "echo(1): slowly") {
		t.Fatalf("got %d %s", code, body)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatalf("answered after %s, want at least 150ms", time.Since(start))
	}
}

func TestSleepPrefixWithBadNumberIsPlainText(t *testing.T) {
	h := New("k").Handler()
	if _, body := ask(t, h, "k", "sleep soon"); !strings.Contains(body, "echo(1): sleep soon") {
		t.Fatalf("got %s", body)
	}
}
