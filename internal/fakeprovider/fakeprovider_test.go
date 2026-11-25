package fakeprovider

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func post(t *testing.T, h http.Handler, path, key string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", path, bytes.NewReader(b))
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("X-Attempt", "1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestModesAndPermanentFailures(t *testing.T) {
	s := New("email")
	h := s.Handler()
	if w := post(t, h, "/v1/send", "a", map[string]any{"notification_id": "a", "to": "x@example.com"}); w.Code != 200 {
		t.Fatalf("ok mode: %d", w.Code)
	}
	if w := post(t, h, "/v1/send", "b", map[string]any{"notification_id": "b", "to": "nobody@invalid.example"}); w.Code != 400 {
		t.Fatalf("invalid recipient: %d", w.Code)
	}
	s.SetMode(Mode{Mode: "fail_5xx"})
	if w := post(t, h, "/v1/send", "c", map[string]any{"notification_id": "c", "to": "x@example.com"}); w.Code != 503 {
		t.Fatalf("fail_5xx: %d", w.Code)
	}
	s.SetMode(Mode{Mode: "fail_429"})
	w := post(t, h, "/v1/send", "d", map[string]any{"notification_id": "d", "to": "x@example.com"})
	if w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("fail_429: %d retry-after=%q", w.Code, w.Header().Get("Retry-After"))
	}
	p := New("push")
	if w := post(t, p.Handler(), "/v1/push", "e", map[string]any{"notification_id": "e", "device_token": "expired-123"}); w.Code != 410 {
		t.Fatalf("expired token: %d", w.Code)
	}
	if got := len(s.Records()); got != 4 {
		t.Fatalf("recorded %d requests, want 4", got)
	}
}

func TestSameKeyIsCollapsed(t *testing.T) {
	s := New("email")
	h := s.Handler()
	for i := 0; i < 3; i++ {
		post(t, h, "/v1/send", "k1", map[string]any{"notification_id": "k1", "to": "x@example.com"})
	}
	st := ComputeStats(s.Records(), 100)
	if st.Delivered != 1 || st.Duplicates != 2 {
		t.Fatalf("delivered=%d duplicates=%d, want 1 and 2", st.Delivered, st.Duplicates)
	}
}

func TestProviderSideRateLimit(t *testing.T) {
	s := New("email")
	s.SetMode(Mode{Mode: "ok", RateLimitRPS: 1, RateLimitBurst: 3})
	h := s.Handler()
	codes := map[int]int{}
	for i := 0; i < 10; i++ {
		codes[post(t, h, "/v1/send", "", map[string]any{"notification_id": "r", "to": "x@example.com"}).Code]++
	}
	if codes[200] != 3 || codes[429] != 7 {
		t.Fatalf("codes %v, want 3×200 then 7×429", codes)
	}
}

func TestFailFirst(t *testing.T) {
	s := New("email")
	s.SetMode(Mode{Mode: "ok", FailFirst: map[string]int{"learn@example.com": 1}})
	h := s.Handler()
	first := post(t, h, "/v1/send", "f", map[string]any{"notification_id": "f", "to": "learn@example.com"})
	second := post(t, h, "/v1/send", "f", map[string]any{"notification_id": "f", "to": "learn@example.com"})
	if first.Code != 503 || second.Code != 200 {
		t.Fatalf("got %d then %d, want 503 then 200", first.Code, second.Code)
	}
}

func TestStatsBuckets(t *testing.T) {
	st := ComputeStats([]Record{{MS: 5, Status: 200}, {MS: 50, Status: 503}, {MS: 250, Status: 200}}, 100)
	if st.Peak != 2 || len(st.Buckets) != 3 || st.Buckets[1].Count != 0 {
		t.Fatalf("stats %+v", st)
	}
}
