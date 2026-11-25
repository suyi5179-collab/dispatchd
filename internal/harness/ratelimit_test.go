package harness_test

import (
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/backoff"
	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/harness"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// The provider's quota is 20 requests a second (burst 5); above it, 429.
// The worker's bucket for that provider is 18/s (burst 5). A backlog of
// 100 notifications arrives at once. With the bucket the worker paces
// itself and the provider almost never says 429.
// @learn:ratelimit-test
func TestTokenBucketKeepsUnderTheProviderQuota(t *testing.T) {
	const n = 100
	s := harness.Start(t, harness.Options{NoWorkers: true, Concurrency: 32,
		Limits:  map[string][2]float64{"email": {18, 5}},
		Backoff: backoff.Policy{Base: 200 * time.Millisecond, Cap: 2 * time.Second, MaxAttempts: 20}})
	s.Email.SetMode(fakeprovider.Mode{Mode: "ok", RateLimitRPS: 20, RateLimitBurst: 5})
	for i := range n {
		s.Submit(email(i))
	}
	t0 := time.Now()
	s.StartWorker("w1")
	c := s.WaitSettled(60 * time.Second)
	st := fakeprovider.ComputeStats(s.Email.Records(), 1000)
	var maxWait float64
	_ = s.Store.DB.QueryRow(`SELECT COALESCE(MAX(JSON_EXTRACT(data,'$.token_wait_ms')),0) FROM notification_events WHERE stage='claimed'`).Scan(&maxWait)
	t.Logf("%d notifications: %v in %v; provider got %d requests, %d of them 429; busiest second %d; longest token wait %.0f ms",
		n, c, time.Since(t0).Round(10*time.Millisecond), st.Total, st.ByStatus[429], st.Peak, maxWait)
	if c[store.StatusDelivered] != n {
		t.Fatalf("delivered %d of %d", c[store.StatusDelivered], n)
	}
	if st.ByStatus[429] > 5 {
		t.Fatalf("the provider answered 429 %d times (limit 5)", st.ByStatus[429])
	}
}

// @learn:end ratelimit-test
