package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/api"
	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/store"
	"github.com/suyi5179-collab/dispatchd/internal/testenv"
	"github.com/suyi5179-collab/dispatchd/internal/worker"
)

const key = "dk_test_only"

type stack struct {
	st    *store.Store
	api   *httptest.Server
	email *fakeprovider.Server
	push  *fakeprovider.Server
}

func newStack(t *testing.T) *stack {
	st := testenv.MySQL(t)
	ctx := context.Background()
	if err := api.SeedClients(ctx, st, "t:"+key+":admin"); err != nil {
		t.Fatal(err)
	}
	email, push := fakeprovider.New("email"), fakeprovider.New("push")
	es, ps := httptest.NewServer(email.Handler()), httptest.NewServer(push.Handler())
	t.Cleanup(es.Close)
	t.Cleanup(ps.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	d := &worker.Deliverer{Store: st, Log: log, Providers: map[string]*provider.Client{
		"email": provider.NewClient("email", es.URL, 2*time.Second), "push": provider.NewClient("push", ps.URL, 2*time.Second)}}
	srv := &api.Server{Store: st, Log: log, Enqueue: func(ctx context.Context, id string) { go d.Deliver(ctx, id) }}
	hs := httptest.NewServer(srv.Routes())
	t.Cleanup(hs.Close)
	return &stack{st: st, api: hs, email: email, push: push}
}

func (s *stack) do(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	code, out, _ := s.doKey(t, method, path, body, fmt.Sprintf("k-%d", rand.Int64()))
	return code, out
}

// doKey sends one request with an explicit Idempotency-Key.
func (s *stack) doKey(t *testing.T, method, path string, body any, idem string) (int, map[string]any, http.Header) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, s.api.URL+path, rd)
	req.Header.Set("Authorization", "Bearer "+key)
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, resp.Header
}

func (s *stack) waitStatus(t *testing.T, id string, want string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, got := s.do(t, "GET", "/v1/notifications/"+id, nil)
		n := got["notification"].(map[string]any)
		if n["status"] == want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: status %v, want %s", id, n["status"], want)
		}
		time.Sleep(50 * time.Millisecond)
	}
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
