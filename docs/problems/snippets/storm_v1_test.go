//go:build ignore

package harness_test

// The first retry-storm test. It passed with jitter switched off, so it
// proved nothing. Two reasons: the 200 notifications were submitted one
// by one (about 1.5 s apart in total), so their first attempts never
// failed together; and the recovered provider had unlimited capacity, so
// a synchronized wave did no damage it could count.
// @learn:storm-v1
func TestRetriesAfterAnOutageDoNotArriveAsOneSpike(t *testing.T) {
	s := harness.Start(t, harness.Options{Concurrency: 64})
	s.Email.SetMode(fakeprovider.Mode{Mode: "fail_5xx"})
	for i := range 200 {
		s.Submit(email(i)) // a worker is already consuming: attempts spread out
	}
	time.Sleep(1500 * time.Millisecond)
	s.Email.SetMode(fakeprovider.Mode{Mode: "ok"}) // no capacity limit
	s.WaitSettled(60 * time.Second)
	if peak := busiest100ms(s.Email.Records()); peak > 200/4 {
		t.Fatalf("retry storm: %d in one 100 ms", peak)
	}
}

// @learn:end storm-v1
