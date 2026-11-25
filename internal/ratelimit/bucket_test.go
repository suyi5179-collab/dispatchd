package ratelimit

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func fake(rate, burst float64) (*Bucket, *clock) {
	c := &clock{t: time.Unix(1_000_000, 0)}
	b := New(rate, burst)
	b.now = c.now
	b.sleep = func(context.Context, time.Duration) error { return nil }
	return b, c
}

func TestBurstThenEvenlySpaced(t *testing.T) {
	b, _ := fake(10, 3)
	var waits []time.Duration
	for range 6 {
		w, _ := b.Reserve()
		waits = append(waits, w)
	}
	want := []time.Duration{0, 0, 0, 100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond}
	for i := range want {
		if d := waits[i] - want[i]; d > time.Microsecond || d < -time.Microsecond {
			t.Fatalf("waits %v, want %v", waits, want)
		}
	}
}

func TestRefillIsCappedAtBurst(t *testing.T) {
	b, c := fake(10, 3)
	for range 3 {
		b.Reserve()
	}
	c.add(time.Hour)
	if got := b.Tokens(); got != 3 {
		t.Fatalf("after an hour idle: %v tokens, want 3", got)
	}
}

// Many goroutines at once: in any one-second window no more than
// Rate + Burst requests are let through.
func TestConcurrentCallersNeverExceedTheRate(t *testing.T) {
	b := New(200, 10) // real clock
	var mu sync.Mutex
	var at []time.Time
	var wg sync.WaitGroup
	for range 300 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = b.Wait(context.Background())
			mu.Lock()
			at = append(at, time.Now())
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(at, func(i, j int) bool { return at[i].Before(at[j]) })
	worst := 0
	for i := range at {
		j := sort.Search(len(at), func(k int) bool { return at[k].After(at[i].Add(500 * time.Millisecond)) })
		worst = max(worst, j-i)
	}
	if worst > 100+10+5 { // 200/s × 0.5 s + burst, plus scheduling slack
		t.Fatalf("%d requests in one 500 ms window", worst)
	}
	if took := at[len(at)-1].Sub(at[0]); took < 1300*time.Millisecond {
		t.Fatalf("300 requests at 200/s finished in %v", took)
	}
}

func TestCancelGivesTheTokenBack(t *testing.T) {
	b := New(1, 1)
	b.Reserve() // empty
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := b.Wait(ctx); err == nil {
		t.Fatal("expected the wait to be cancelled")
	}
	if tok := b.Tokens(); tok < -0.5 {
		t.Fatalf("token not refunded: %v", tok)
	}
}
