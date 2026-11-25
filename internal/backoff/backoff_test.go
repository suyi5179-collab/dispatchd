package backoff

import (
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/config"
)

func TestCeilingDoublesAndCaps(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Cap: time.Second, MaxAttempts: 8}
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := p.Ceiling(i + 1); got != w*time.Millisecond {
			t.Errorf("attempt %d: ceiling %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
}

func TestFullJitterStaysUnderTheCeilingAndSpreads(t *testing.T) {
	if config.Sabotaged(config.NoJitter) {
		t.Skip("jitter is sabotaged in this run")
	}
	p := Policy{Base: 100 * time.Millisecond, Cap: 10 * time.Second}
	lo, hi := time.Hour, time.Duration(0)
	for range 2000 {
		d, c := p.Next(4, 0)
		if d < 0 || d > 800*time.Millisecond || c.Jitter != "full" {
			t.Fatalf("delay %v outside [0, 800ms] (%+v)", d, c)
		}
		lo, hi = min(lo, d), max(hi, d)
	}
	if lo > 80*time.Millisecond || hi < 720*time.Millisecond {
		t.Fatalf("2000 draws only covered [%v, %v] of [0, 800ms]", lo, hi)
	}
}

func TestRetryAfterIsAFloor(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Cap: time.Second, Rand: func() float64 { return 0.5 }}
	d, c := p.Next(1, 2*time.Second)
	if config.Sabotaged(config.NoJitter) {
		if d != 2100*time.Millisecond {
			t.Fatalf("got %v", d)
		}
		return
	}
	if d != 2050*time.Millisecond || c.RetryAfterMS != 2000 {
		t.Fatalf("got %v %+v", d, c)
	}
}
