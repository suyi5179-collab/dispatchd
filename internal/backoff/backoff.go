// Package backoff decides how long to wait before the next attempt.
//
// Exponential backoff with full jitter: the ceiling doubles with every
// attempt (base·2^(n-1), capped), and the actual wait is drawn uniformly
// from [0, ceiling]. Without the random draw, every notification that
// failed in the same outage retries at the same instant, and the provider
// that just came back gets the whole backlog in one spike.
package backoff

import (
	"math/rand/v2"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/config"
)

// Policy of retries.
type Policy struct {
	Base        time.Duration
	Cap         time.Duration
	MaxAttempts int            // total attempts, including the first
	Rand        func() float64 // [0,1); nil means math/rand/v2
}

// Default policy: 1 s, 2 s, 4 s … capped at 2 min, eight attempts.
func Default() Policy {
	return Policy{Base: time.Second, Cap: 2 * time.Minute, MaxAttempts: 8}
}

// Calc explains one decision; it is stored with the retry so a person
// (and the learn page) can see how the wait was chosen.
type Calc struct {
	Attempt      int     `json:"attempt"` // the attempt that just failed
	BaseMS       float64 `json:"base_ms"`
	CapMS        float64 `json:"cap_ms"`
	CeilingMS    float64 `json:"ceiling_ms"` // min(cap, base·2^(attempt-1))
	Jitter       string  `json:"jitter"`     // "full" or "none"
	Rand         float64 `json:"rand,omitempty"`
	RetryAfterMS float64 `json:"retry_after_ms,omitempty"` // the provider's hint, added on top
	DelayMS      float64 `json:"delay_ms"`
}

// Ceiling is the largest wait after the given failed attempt.
func (p Policy) Ceiling(attempt int) time.Duration {
	d := p.Base
	for i := 1; i < attempt && d < p.Cap; i++ {
		d *= 2
	}
	return min(d, p.Cap)
}

// Next returns the wait after a failed attempt. retryAfter (from a 429)
// is a floor: the provider asked us not to come back sooner.
// @learn:full-jitter
func (p Policy) Next(attempt int, retryAfter time.Duration) (time.Duration, Calc) {
	ceil := p.Ceiling(attempt)
	c := Calc{Attempt: attempt, BaseMS: ms(p.Base), CapMS: ms(p.Cap), CeilingMS: ms(ceil), RetryAfterMS: ms(retryAfter)}
	var d time.Duration
	if config.Sabotaged(config.NoJitter) {
		// @learn:no-jitter
		c.Jitter = "none"
		d = ceil // everyone who failed together comes back together
		// @learn:end no-jitter
	} else {
		r := rand.Float64
		if p.Rand != nil {
			r = p.Rand
		}
		c.Jitter = "full"
		c.Rand = r()
		d = time.Duration(c.Rand * float64(ceil))
	}
	d += retryAfter
	c.DelayMS = ms(d)
	return d, c
}

// @learn:end full-jitter

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
