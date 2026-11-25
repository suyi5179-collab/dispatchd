package api_test

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/harness"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

const key = harness.Key

type stack struct {
	*harness.Stack
	st    *store.Store
	api   *httptest.Server
	email *fakeprovider.Server
	push  *fakeprovider.Server
}

func newStack(t *testing.T) *stack {
	h := harness.Start(t, harness.Options{})
	return &stack{Stack: h, st: h.Store, api: h.API, email: h.Email, push: h.Push}
}

func (s *stack) do(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	code, out, _ := s.Do(method, path, body, fmt.Sprintf("k-%d", rand.Int64()))
	return code, out
}

func (s *stack) doKey(t *testing.T, method, path string, body any, idem string) (int, map[string]any, http.Header) {
	t.Helper()
	return s.Do(method, path, body, idem)
}

func (s *stack) waitStatus(t *testing.T, id string, want string) map[string]any {
	t.Helper()
	return s.WaitStatus(id, want, 15*time.Second)
}

func TestSubmitAndDeliver(t *testing.T) {
	s := newStack(t)
	code, body := s.do(t, "POST", "/v1/notifications", map[string]any{"channel": "email", "recipient": "ada@example.com", "template": "welcome", "params": map[string]any{"name": "Ada"}})
	if code != 202 {
		t.Fatalf("POST: %d %v", code, body)
	}
	got := s.waitStatus(t, body["id"].(string), store.StatusDelivered)
	if n := len(got["attempts"].([]any)); n != 1 {
		t.Fatalf("%d attempts, want 1", n)
	}
	if recs := s.email.Records(); len(recs) != 1 || recs[0].NotificationID != body["id"] {
		t.Fatalf("provider saw %+v", recs)
	}
}

func TestPermanentFailureIsRecordedWithAnAction(t *testing.T) {
	s := newStack(t)
	_, body := s.do(t, "POST", "/v1/notifications", map[string]any{"channel": "push", "recipient": "expired-device-1", "template": "otp", "params": map[string]any{"code": "1"}})
	got := s.waitStatus(t, body["id"].(string), store.StatusFailedPermanent)
	le := got["notification"].(map[string]any)["last_error"].(map[string]any)
	if le["code"] != "device_token_unregistered" || le["action"] == "" || le["http_status"].(float64) != 410 {
		t.Fatalf("last_error %v", le)
	}
}

func TestValidationAndAuth(t *testing.T) {
	s := newStack(t)
	if code, _ := s.do(t, "POST", "/v1/notifications", map[string]any{"channel": "sms", "recipient": "x", "template": "welcome"}); code != 400 {
		t.Fatalf("bad channel: %d", code)
	}
	if code, _ := s.do(t, "POST", "/v1/notifications", map[string]any{"channel": "email", "recipient": "x@example.com", "template": "nope"}); code != 400 {
		t.Fatalf("bad template: %d", code)
	}
	req, _ := http.NewRequest("GET", s.api.URL+"/v1/notifications", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 401 {
		t.Fatalf("wrong key: %d", resp.StatusCode)
	}
}
