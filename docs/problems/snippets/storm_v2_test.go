//go:build ignore

package harness_test

// The storm test as it stood until the first CI run on GitHub (see
// docs/problems.md, q11). stormPeak is unchanged in
// internal/harness/retry_test.go; the assertion at the bottom used a
// fixed limit: at most 60 of the 200 notifications rejected with 429.
// That held on the laptop (29) and failed on the CI runner (109).
// @learn:storm-v2
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

// @learn:end storm-v2
