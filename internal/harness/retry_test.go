package harness_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/backoff"
	"github.com/suyi5179-collab/dispatchd/internal/config"
	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/harness"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

func email(i int) map[string]any {
	return map[string]any{"channel": "email", "recipient": fmt.Sprintf("user%d@example.com", i), "template": "welcome", "params": map[string]any{"name": i}}
}

// The provider answers 503 to everything for a while, then recovers.
// Every notification must end up delivered, after more than one attempt.
func TestProviderOutageThenRecoveryDeliversEverything(t *testing.T) {
	s := harness.Start(t, harness.Options{Backoff: backoff.Policy{Base: 100 * time.Millisecond, Cap: time.Second, MaxAttempts: 20}})
	s.Email.SetMode(fakeprovider.Mode{Mode: "fail_5xx"})
	for i := range 50 {
		s.Submit(email(i))
	}
	time.Sleep(1500 * time.Millisecond)
	if c := s.Counts(); c[store.StatusDelivered] != 0 || c[store.StatusRetryScheduled] == 0 {
		t.Fatalf("during the outage: %v", c)
	}
	s.Email.SetMode(fakeprovider.Mode{Mode: "ok"})
	c := s.WaitSettled(30 * time.Second)
	var multi int
	_ = s.Store.DB.QueryRow(`SELECT COUNT(*) FROM notifications WHERE attempts > 1`).Scan(&multi)
	st := fakeprovider.ComputeStats(s.Email.Records(), 100)
	t.Logf("after recovery: %v; %d needed more than one attempt; provider got %d requests (%v)", c, multi, st.Total, st.ByStatus)
	if c[store.StatusDelivered] != 50 || multi != 50 {
		t.Fatalf("delivered %d of 50, %d retried", c[store.StatusDelivered], multi)
	}
}

func TestTemporaryFailuresStopAtMaxAttempts(t *testing.T) {
	s := harness.Start(t, harness.Options{Backoff: backoff.Policy{Base: 20 * time.Millisecond, Cap: 100 * time.Millisecond, MaxAttempts: 3}})
	s.Email.SetMode(fakeprovider.Mode{Mode: "fail_5xx"})
	id := s.Submit(email(1))
	got := s.WaitStatus(id, store.StatusFailedExhausted, 10*time.Second)
	if n := len(got["attempts"].([]any)); n != 3 {
		t.Fatalf("%d attempts, want 3", n)
	}
	le := got["notification"].(map[string]any)["last_error"].(map[string]any)
	if le["http_status"].(float64) != 503 {
		t.Fatalf("last_error %v", le)
	}
}

// A worker stops while retries are waiting in its delay queue (memory
// only). A new worker must find them in MySQL and deliver them.
// @learn:reload-test
func TestNewWorkerReloadsRetriesFromMySQL(t *testing.T) {
	s := harness.Start(t, harness.Options{Backoff: backoff.Policy{Base: 2 * time.Second, Cap: 2 * time.Second, MaxAttempts: 5}})
	s.Email.SetMode(fakeprovider.Mode{Mode: "fail_5xx"})
	for i := range 20 {
		s.Submit(email(i))
	}
	deadline := time.Now().Add(10 * time.Second)
	for s.Counts()[store.StatusRetryScheduled] < 20 {
		if time.Now().After(deadline) {
			t.Fatalf("not all scheduled: %v", s.Counts())
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.StopWorker("w1") // its delay queue is gone
	s.Email.SetMode(fakeprovider.Mode{Mode: "ok"})
	s.StartWorker("w2")
	c := s.WaitSettled(20 * time.Second)
	if c[store.StatusDelivered] != 20 {
		t.Fatalf("after restart: %v", c)
	}
}

// @learn:end reload-test

// stormPeak builds a backlog of n notifications (the worker is down, or
// a campaign was queued in one go), then starts the worker while the
// provider is down. All first attempts fail within a few milliseconds of
// each other. The provider recovers 1.5 s later, but like a real service
// that just came back it can only take 100 requests a second (burst 10);
// anything above that gets 429. It returns what the provider saw after
// recovering: the busiest 100 ms, the 429s it had to send, and the
// requests per 100 ms.
// @learn:storm-test
func stormPeak(t *testing.T, n int) (peak, rejected int, hist string, total int, took time.Duration) {
	s := harness.Start(t, harness.Options{NoWorkers: true, Concurrency: 64, Backoff: backoff.Policy{Base: 500 * time.Millisecond, Cap: 8 * time.Second, MaxAttempts: 12}})
	for i := range n {
		s.Submit(email(i))
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if u, _ := s.Store.Unpublished(t.Context()); u == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox not drained")
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.Email.SetMode(fakeprovider.Mode{Mode: "fail_5xx"})
	t0 := time.Now()
	s.StartWorker("w1")
	time.Sleep(1500 * time.Millisecond) // the outage
	recovered := s.Email.ElapsedMS()
	s.Email.SetMode(fakeprovider.Mode{Mode: "ok", RateLimitRPS: 100, RateLimitBurst: 10})
	c := s.WaitSettled(60 * time.Second)
	took = time.Since(t0)
	if c[store.StatusDelivered] != n {
		t.Fatalf("delivered %v", c)
	}
	var after []fakeprovider.Record
	for _, r := range s.Email.Records() {
		if r.MS >= recovered {
			after = append(after, r)
		}
	}
	st := fakeprovider.ComputeStats(after, 100)
	saveStorm(t, n, recovered, after, st, took)
	var b strings.Builder
	for i, k := range st.Buckets {
		if i >= 40 {
			break
		}
		fmt.Fprintf(&b, "%d ", k.Count)
	}
	return st.Peak, st.ByStatus[429], b.String(), len(s.Email.Records()), took
}

func TestRetriesAfterAnOutageDoNotArriveAsOneSpike(t *testing.T) {
	const n = 200
	peak, rejected, hist, total, took := stormPeak(t, n)
	t.Logf("%d notifications failed together; after recovery: busiest 100 ms %d requests, %d rejected with 429", n, peak, rejected)
	t.Logf("requests per 100 ms after recovery: %s", hist)
	t.Logf("provider requests in total: %d; all delivered after %v", total, took.Round(10*time.Millisecond))
	if rejected > n*3/10 {
		t.Fatalf("retry storm: the recovering provider had to reject %d requests (limit %d)", rejected, n*3/10)
	}
}

// @learn:end storm-test

// saveStorm writes the run's provider-side histogram when
// DISPATCHD_RESULTS_DIR is set (scripts/contrasts.sh), for the README
// and the UI's providers page.
func saveStorm(t *testing.T, n int, recovered int64, after []fakeprovider.Record, st fakeprovider.Stats, took time.Duration) {
	dir := os.Getenv("DISPATCHD_RESULTS_DIR")
	if dir == "" {
		return
	}
	type bucket struct {
		TMS      int64 `json:"t_ms"`
		Count    int   `json:"count"`
		Rejected int   `json:"rejected"`
	}
	cnt, rej := map[int64]int{}, map[int64]int{}
	var last int64
	for _, r := range after {
		k := (r.MS - recovered) / 100
		cnt[k]++
		if r.Status == 429 {
			rej[k]++
		}
		last = max(last, k)
	}
	var bs []bucket
	for k := int64(0); k <= last; k++ {
		bs = append(bs, bucket{TMS: k * 100, Count: cnt[k], Rejected: rej[k]})
	}
	jitter := !config.Sabotaged(config.NoJitter)
	out := map[string]any{"jitter": jitter, "notifications": n, "outage_ms": 1500, "provider_capacity_rps": 100,
		"after_recovery":         map[string]any{"requests": st.Total, "peak_per_100ms": st.Peak, "rejected_429": st.ByStatus[429], "buckets": bs},
		"all_delivered_after_ms": took.Milliseconds()}
	name := "storm-jitter.json"
	if !jitter {
		name = "storm-nojitter.json"
	}
	b, _ := json.MarshalIndent(out, "", " ")
	if err := os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o644); err != nil {
		t.Log(err)
	}
}
