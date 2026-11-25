// Package ratelimit is a token bucket: Rate tokens per second, up to
// Burst saved up. Each provider gets one, set a little under its quota,
// so a worker waits for a token instead of sending a request the provider
// will answer with 429.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/config"
)

// Bucket is safe for concurrent use.
type Bucket struct {
	Rate  float64 // tokens per second
	Burst float64 // capacity

	mu     sync.Mutex
	tokens float64
	last   time.Time
	now    func() time.Time
	sleep  func(context.Context, time.Duration) error
}

// New makes a full bucket.
func New(rate, burst float64) *Bucket {
	return &Bucket{Rate: rate, Burst: max(burst, 1), tokens: max(burst, 1), now: time.Now, sleep: sleepCtx}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// refill adds the tokens earned since the last call. Caller holds mu.
func (b *Bucket) refill(now time.Time) {
	if !b.last.IsZero() {
		b.tokens = min(b.Burst, b.tokens+now.Sub(b.last).Seconds()*b.Rate)
	}
	b.last = now
}

// Reserve takes one token now and says how long to wait before using it.
// The count may go negative: that is the queue of callers already
// waiting, so each new caller waits behind them, evenly spaced at 1/Rate.
// @learn:token-bucket
func (b *Bucket) Reserve() (wait time.Duration, left float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(b.now())
	b.tokens--
	if b.tokens >= 0 {
		return 0, b.tokens
	}
	return time.Duration(-b.tokens / b.Rate * float64(time.Second)), b.tokens
}

// Wait blocks until the caller may send. If ctx ends first, the token
// is given back.
func (b *Bucket) Wait(ctx context.Context) (waited time.Duration, left float64, err error) {
	// @learn:ratelimit-sabotage
	if b == nil || config.Sabotaged(config.NoRateLimit) {
		return 0, 0, nil // no bucket: every request goes straight out
	}
	// @learn:end ratelimit-sabotage
	wait, left := b.Reserve()
	if wait <= 0 {
		return 0, left, nil
	}
	if err := b.sleep(ctx, wait); err != nil {
		b.Refund()
		return 0, left, err
	}
	return wait, left, nil
}

// @learn:end token-bucket

// Refund returns a token that was reserved but not used.
func (b *Bucket) Refund() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tokens = min(b.Burst, b.tokens+1)
	b.mu.Unlock()
}

// Tokens is the current balance (negative: callers are waiting).
func (b *Bucket) Tokens() float64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(b.now())
	return b.tokens
}
