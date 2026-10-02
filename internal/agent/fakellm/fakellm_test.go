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

func TestAnswersCarryDeterministicUsage(t *testing.T) {
	s := New("k")
	h := s.Handler()
	// "hello world" is 11 bytes: 3 tokens plus 4 of framing for the prompt;
	// "echo(1): hello world" is 20 bytes: 5 tokens.
	_, body := ask(t, h, "k", "hello world")
	if !strings.Contains(body, `"usage":{"completion_tokens":5,"prompt_tokens":7,"total_tokens":12}`) {
		t.Fatalf("usage: %s", body)
	}
	if prompt, completion := s.Tokens(); prompt != 7 || completion != 5 {
		t.Fatalf("booked %d/%d", prompt, completion)
	}
}

func TestOmitUsageDropsTheObjectButStillBooksTheCost(t *testing.T) {
	s := New("k")
	s.OmitUsage(true)
	_, body := ask(t, s.Handler(), "k", "hello world")
	if strings.Contains(body, "usage") {
		t.Fatalf("usage must be omitted: %s", body)
	}
	if prompt, _ := s.Tokens(); prompt != 7 {
		t.Fatalf("cost not booked: %d", prompt)
	}
}

func TestRejectedRequestsCostNothing(t *testing.T) {
	s := New("k")
	ask(t, s.Handler(), "bad", "hello world")
	if prompt, completion := s.Tokens(); prompt != 0 || completion != 0 {
		t.Fatalf("a refused request was booked: %d/%d", prompt, completion)
	}
}

func TestRecallFindsOnlyEarlierMessages(t *testing.T) {
	h := New("k").Handler()
	post := func(msgs string) string {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"messages":`+msgs+`}`))
		req.Header.Set("Authorization", "Bearer k")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Body.String()
	}
	found := post(`[{"role":"user","content":"my code is zx81"},{"role":"assistant","content":"ok"},{"role":"user","content":"recall zx81"}]`)
	if !strings.Contains(found, "recall(found): zx81") {
		t.Fatalf("earlier message not found: %s", found)
	}
	missing := post(`[{"role":"user","content":"recall zx81"}]`)
	if !strings.Contains(missing, "recall(missing): zx81") {
		t.Fatalf("the request itself must not count as history: %s", missing)
	}
}
