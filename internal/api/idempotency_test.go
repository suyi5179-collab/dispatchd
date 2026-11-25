package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

var welcome = map[string]any{"channel": "email", "recipient": "ada@example.com", "template": "welcome", "params": map[string]any{"name": "Ada"}}

func TestSameKeySameBodyReturnsTheSameNotification(t *testing.T) {
	s := newStack(t)
	c1, b1, _ := s.doKey(t, "POST", "/v1/notifications", welcome, "order-42-welcome")
	c2, b2, h2 := s.doKey(t, "POST", "/v1/notifications", welcome, "order-42-welcome")
	if c1 != 202 || c2 != 202 {
		t.Fatalf("codes %d %d", c1, c2)
	}
	if b1["id"] != b2["id"] {
		t.Fatalf("same key gave two notifications: %v and %v", b1["id"], b2["id"])
	}
	if h2.Get("Idempotent-Replayed") != "true" {
		t.Fatal("replay not marked")
	}
	s.waitStatus(t, b1["id"].(string), store.StatusDelivered)
	time.Sleep(200 * time.Millisecond)
	if st := fakeprovider.ComputeStats(s.email.Records(), 100); st.Total != 1 {
		t.Fatalf("provider received %d requests, want 1", st.Total)
	}
}

func TestSameKeyDifferentBodyIs422(t *testing.T) {
	s := newStack(t)
	_, b1, _ := s.doKey(t, "POST", "/v1/notifications", welcome, "k-reuse")
	other := map[string]any{"channel": "email", "recipient": "bob@example.com", "template": "welcome", "params": map[string]any{"name": "Bob"}}
	code, b2, _ := s.doKey(t, "POST", "/v1/notifications", other, "k-reuse")
	if code != 422 || b2["notification_id"] != b1["id"] {
		t.Fatalf("got %d %v", code, b2)
	}
}

func TestMissingKeyIs400(t *testing.T) {
	s := newStack(t)
	if code, _, _ := s.doKey(t, "POST", "/v1/notifications", welcome, ""); code != 400 {
		t.Fatalf("got %d", code)
	}
}

// Twenty requests with one key at the same moment: one row, one id.
func TestConcurrentSameKeyStoresOneRow(t *testing.T) {
	s := newStack(t)
	const n = 20
	ids := make([]string, n)
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			code, b, _ := s.doKey(t, "POST", "/v1/notifications", welcome, "k-race")
			codes[i] = code
			ids[i], _ = b["id"].(string)
		}()
	}
	close(start)
	wg.Wait()
	for i := range n {
		if codes[i] != 202 || ids[i] != ids[0] {
			t.Fatalf("request %d: %d %q (first was %q)", i, codes[i], ids[i], ids[0])
		}
	}
	var rows int
	_ = s.st.DB.QueryRow(`SELECT COUNT(*) FROM notifications`).Scan(&rows)
	if rows != 1 {
		t.Fatalf("%d notifications stored, want 1", rows)
	}
}

// The caller's HTTP client gives up after 100 ms; the server finishes the
// work but its answer arrives 300 ms later and is lost. The caller does
// what every client library does: retry with the same key. The person
// must get one email.
// @learn:timeout-retry-test
func TestRetryAfterTimeoutSendsOnce(t *testing.T) {
	s := newStack(t)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		s.api.Config.Handler.ServeHTTP(rec, r) // the work happens…
		time.Sleep(300 * time.Millisecond)     // …but the answer is late
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	defer slow.Close()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	send := func() (*http.Response, error) {
		b, _ := json.Marshal(welcome)
		req, _ := http.NewRequest("POST", slow.URL+"/v1/notifications", bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Idempotency-Key", "signup-ada-1")
		return client.Do(req)
	}
	if _, err := send(); err == nil || !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err) {
		t.Fatalf("first call should time out on the caller side, got %v", err)
	}
	client.Timeout = 2 * time.Second
	resp, err := send()
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var pending int
		_ = s.st.DB.QueryRow(`SELECT COUNT(*) FROM notifications WHERE status IN ('QUEUED','SENDING')`).Scan(&pending)
		if pending == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	var rows int
	_ = s.st.DB.QueryRow(`SELECT COUNT(*) FROM notifications`).Scan(&rows)
	st := fakeprovider.ComputeStats(s.email.Records(), 100)
	t.Logf("notifications stored: %d, provider deliveries: %d", rows, st.Delivered)
	if st.Delivered != 1 {
		t.Fatalf("the provider delivered %d emails for one signup (notifications stored: %d)", st.Delivered, rows)
	}
}

// @learn:end timeout-retry-test

func isTimeout(err error) bool {
	var te interface{ Timeout() bool }
	return errors.As(err, &te) && te.Timeout()
}
