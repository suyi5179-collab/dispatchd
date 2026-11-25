// Package harness runs the whole pipeline inside one test process: the
// API and its outbox relay, workers, and both fake providers, against a
// throwaway MySQL database and throwaway Kafka topics. Tests use it to
// make things go wrong on purpose (provider outages, killed workers,
// duplicate requests) and check what the providers actually received.
package harness

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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/api"
	"github.com/suyi5179-collab/dispatchd/internal/backoff"
	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/queue"
	"github.com/suyi5179-collab/dispatchd/internal/ratelimit"
	"github.com/suyi5179-collab/dispatchd/internal/relay"
	"github.com/suyi5179-collab/dispatchd/internal/store"
	"github.com/suyi5179-collab/dispatchd/internal/testenv"
	"github.com/suyi5179-collab/dispatchd/internal/worker"
)

// Key is the API key the harness registers.
const Key = "dk_test_only"

// Options tune the stack.
type Options struct {
	Workers         int            // in-process workers started by Start (default 1; 0 with NoWorkers)
	NoWorkers       bool           // start none; the test starts them
	Lease           time.Duration  // default 2s
	ProviderTimeout time.Duration  // default 2s
	Concurrency     int            // per worker, default 32
	Backoff         backoff.Policy // default: 50 ms base, 1 s cap, 8 attempts
	// Limits: provider name → {rate per second, burst}. Empty: no buckets.
	Limits map[string][2]float64
}

// Stack is a running pipeline.
type Stack struct {
	T        testing.TB
	Opt      Options
	Store    *store.Store
	DSN      string
	API      *httptest.Server
	Handler  http.Handler
	Email    *fakeprovider.Server
	Push     *fakeprovider.Server
	EmailURL string
	PushURL  string
	Brokers  []string
	Topics   queue.Topics
	Group    string
	Log      *slog.Logger

	mu      sync.Mutex
	workers map[string]context.CancelFunc
}

// Start brings the stack up; t.Cleanup tears it down.
func Start(t testing.TB, opt Options) *Stack {
	t.Helper()
	if opt.Lease == 0 {
		opt.Lease = 2 * time.Second
	}
	if opt.ProviderTimeout == 0 {
		opt.ProviderTimeout = 2 * time.Second
	}
	if opt.Concurrency == 0 {
		opt.Concurrency = 32
	}
	if opt.Backoff.MaxAttempts == 0 {
		opt.Backoff = backoff.Policy{Base: 50 * time.Millisecond, Cap: time.Second, MaxAttempts: 8}
	}
	if opt.Workers == 0 && !opt.NoWorkers {
		opt.Workers = 1
	}
	st := testenv.MySQL(t)
	brokers, topics := testenv.Kafka(t)
	logw := io.Discard
	if os.Getenv("DISPATCHD_TEST_LOG") == "1" {
		logw = os.Stderr
	}
	s := &Stack{T: t, Opt: opt, Store: st, DSN: testenv.DSN(t, st), Brokers: brokers, Topics: topics,
		Group: "g-" + topics.Prefix, Log: slog.New(slog.NewTextHandler(logw, nil)), workers: map[string]context.CancelFunc{}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := api.SeedClients(ctx, st, "t:"+Key+":admin"); err != nil {
		t.Fatal(err)
	}
	s.Email, s.Push = fakeprovider.New("email"), fakeprovider.New("push")
	es, ps := httptest.NewServer(s.Email.Handler()), httptest.NewServer(s.Push.Handler())
	t.Cleanup(es.Close)
	t.Cleanup(ps.Close)
	s.EmailURL, s.PushURL = es.URL, ps.URL

	kc, err := queue.Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kc.Close)
	rl := relay.New(st, kc, s.Log)
	go rl.Run(ctx)
	srv := &api.Server{Store: st, Log: s.Log, Topics: topics, Enqueue: func(context.Context, string) { rl.Wake() }}
	s.Handler = srv.Routes()
	s.API = httptest.NewServer(s.Handler)
	t.Cleanup(s.API.Close)
	for i := 0; i < opt.Workers; i++ {
		s.StartWorker(fmt.Sprintf("w%d", i+1))
	}
	t.Cleanup(s.StopAll)
	return s
}

// Deliverer builds a worker's deliverer.
func (s *Stack) Deliverer(name string) *worker.Deliverer {
	return (&worker.Deliverer{Store: s.Store, Log: s.Log, Name: name, Lease: s.Opt.Lease, Backoff: s.Opt.Backoff, Concurrency: s.Opt.Concurrency,
		Limits: s.limits(),
		Providers: map[string]*provider.Client{
			"email": provider.NewClient("email", s.EmailURL, s.Opt.ProviderTimeout),
			"push":  provider.NewClient("push", s.PushURL, s.Opt.ProviderTimeout),
		}}).Init()
}

// limits builds one bucket per configured provider. Each worker gets its
// own buckets, like separate processes would.
func (s *Stack) limits() map[string]*ratelimit.Bucket {
	m := map[string]*ratelimit.Bucket{}
	for name, l := range s.Opt.Limits {
		m[name] = ratelimit.New(l[0], l[1])
	}
	return m
}

// StartWorker starts an in-process worker.
func (s *Stack) StartWorker(name string) {
	s.T.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	d := s.Deliverer(name)
	kc, err := worker.ConsumerClient(s.Brokers, s.Group, s.Topics.All(), 6*time.Second, func() { go func() { _, _ = d.Reload(ctx) }() })
	if err != nil {
		s.T.Fatal(err)
	}
	c := &worker.Consumer{Client: kc, D: d, Log: s.Log, Concurrency: s.Opt.Concurrency, Name: name}
	done := make(chan struct{})
	go func() {
		defer close(done)
		go d.RunScheduler(ctx)
		if err := c.Run(ctx); err != nil {
			s.T.Logf("worker %s stopped: %v", name, err)
		}
	}()
	s.mu.Lock()
	s.workers[name] = func() { cancel(); <-done; kc.Close() }
	s.mu.Unlock()
}

// StopWorker stops one in-process worker gracefully.
func (s *Stack) StopWorker(name string) {
	s.mu.Lock()
	stop := s.workers[name]
	delete(s.workers, name)
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// StopAll stops every in-process worker.
func (s *Stack) StopAll() {
	s.mu.Lock()
	names := make([]string, 0, len(s.workers))
	for n := range s.workers {
		names = append(names, n)
	}
	s.mu.Unlock()
	for _, n := range names {
		s.StopWorker(n)
	}
}

// Do sends an API request with an Idempotency-Key ("" sends none).
func (s *Stack) Do(method, path string, body any, idem string) (int, map[string]any, http.Header) {
	s.T.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, s.API.URL+path, rd)
	req.Header.Set("Authorization", "Bearer "+Key)
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.T.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, resp.Header
}

// Submit posts one notification with a fresh key and returns its id.
func (s *Stack) Submit(body map[string]any) string {
	s.T.Helper()
	code, out, _ := s.Do("POST", "/v1/notifications", body, fmt.Sprintf("k-%d", rand.Int64()))
	if code != 202 {
		s.T.Fatalf("submit: %d %v", code, out)
	}
	return out["id"].(string)
}

// Get returns GET /v1/notifications/{id}.
func (s *Stack) Get(id string) map[string]any {
	s.T.Helper()
	_, out, _ := s.Do("GET", "/v1/notifications/"+id, nil, "")
	return out
}

// WaitStatus polls until the notification reaches status.
func (s *Stack) WaitStatus(id, status string, timeout time.Duration) map[string]any {
	s.T.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := s.Get(id)
		n, _ := got["notification"].(map[string]any)
		if n != nil && n["status"] == status {
			return got
		}
		if time.Now().After(deadline) {
			s.T.Fatalf("%s: status %v after %v, want %s", id, n["status"], timeout, status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Counts returns notifications per status.
func (s *Stack) Counts() map[string]int {
	s.T.Helper()
	rows, err := s.Store.DB.Query(`SELECT status, COUNT(*) FROM notifications GROUP BY status`)
	if err != nil {
		s.T.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		_ = rows.Scan(&st, &n)
		out[st] = n
	}
	return out
}

// WaitSettled waits until no notification is QUEUED, SENDING or waiting
// for a retry, and returns the counts.
func (s *Stack) WaitSettled(timeout time.Duration) map[string]int {
	s.T.Helper()
	deadline := time.Now().Add(timeout)
	for {
		c := s.Counts()
		if c[store.StatusQueued]+c[store.StatusSending]+c[store.StatusRetryScheduled] == 0 && len(c) > 0 {
			return c
		}
		if time.Now().After(deadline) {
			s.T.Fatalf("not settled after %v: %v", timeout, c)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
