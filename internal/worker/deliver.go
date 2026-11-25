// Package worker delivers notifications: claim an attempt in MySQL, send
// it to the provider, record the result, and schedule a retry in the
// delay queue when the failure is temporary.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/backoff"
	"github.com/suyi5179-collab/dispatchd/internal/config"
	"github.com/suyi5179-collab/dispatchd/internal/delayq"
	"github.com/suyi5179-collab/dispatchd/internal/metrics"
	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/ratelimit"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// Deliverer runs attempts for notifications.
type Deliverer struct {
	Store     *store.Store
	Providers map[string]*provider.Client // by channel
	Log       *slog.Logger
	Name      string // worker name, written into events
	// Lease is how long a claimed attempt belongs to this worker. It must
	// be longer than the provider timeout.
	Lease   time.Duration
	Backoff backoff.Policy
	Delay   *delayq.Queue
	// Concurrency bounds attempts started by the delay queue.
	Concurrency int
	// Limits holds one token bucket per provider (nil: unlimited).
	Limits map[string]*ratelimit.Bucket

	sem chan struct{}
}

// Init fills defaults. Call before use.
func (d *Deliverer) Init() *Deliverer {
	if d.Lease <= 0 {
		d.Lease = 15 * time.Second
	}
	if d.Backoff.MaxAttempts == 0 {
		d.Backoff = backoff.Default()
	}
	if d.Delay == nil {
		d.Delay = delayq.New()
	}
	if d.Concurrency <= 0 {
		d.Concurrency = 32
	}
	d.sem = make(chan struct{}, d.Concurrency)
	return d
}

// Deliver makes the next attempt for a notification, if it needs one now.
// It returns skipped=true when another worker (or an earlier copy of the
// same Kafka message) already owns that attempt, the notification is
// finished, or its retry is not due yet (then it goes into the delay queue).
// @learn:deliver
func (d *Deliverer) Deliver(ctx context.Context, id string) (skipped bool, err error) {
	n, err := d.Store.Notification(ctx, "", id)
	if err != nil {
		return false, err
	}
	switch n.Status {
	case store.StatusQueued:
	case store.StatusRetryScheduled:
		if n.NextAttempt != nil && time.Now().Before(*n.NextAttempt) {
			d.Delay.Push(delayq.Item{ID: id, Due: *n.NextAttempt, Kind: "retry", Attempts: n.Attempts})
			return true, nil
		}
	default:
		return true, nil
	}
	client := d.Providers[n.Channel]
	// Wait for a token before claiming, so the wait does not eat into the
	// lease. A token taken by a worker that then loses the claim goes back.
	bucket := d.Limits[client.Name]
	waited, left, err := bucket.Wait(ctx)
	if err != nil {
		return false, err
	}
	token := map[string]any{"token_wait_ms": float64(waited) / float64(time.Millisecond), "tokens_left": left, "rate_limited_by_us": bucket != nil && !config.Sabotaged(config.NoRateLimit)}
	if bucket != nil {
		token["bucket_rate"], token["bucket_burst"] = bucket.Rate, bucket.Burst
	}
	ObserveTokenWait(client.Name, waited)
	ok, err := d.Store.Claim(ctx, id, n.Attempts, client.Name, d.Lease, token)
	if err != nil || !ok {
		bucket.Refund()
		return !ok, err
	}
	attempt := n.Attempts + 1
	var params map[string]any
	_ = json.Unmarshal(n.Params, &params)
	res := client.Send(ctx, provider.Message{NotificationID: id, Attempt: attempt, Recipient: n.Recipient, Template: n.Template, Params: params})
	// @learn:end deliver
	return false, d.record(ctx, n, client.Name, attempt, res)
}

// record writes the outcome and, for a temporary failure, schedules the
// retry: the due time goes into MySQL first (so a restart finds it), then
// into this worker's delay queue.
// @learn:schedule-retry
func (d *Deliverer) record(ctx context.Context, n *store.Notification, name string, attempt int, res provider.Result) error {
	o := store.Outcome{Attempt: attempt, Outcome: string(res.Kind), HTTPStatus: res.HTTPStatus, ProviderCode: res.Code, Detail: res, LatencyMS: float64(res.Latency) / float64(time.Millisecond)}
	ev := map[string]any{"provider_response": res, "worker": d.Name}
	var due time.Time
	switch res.Kind {
	case provider.Delivered:
		o.NewStatus = store.StatusDelivered
	case provider.Permanent:
		o.NewStatus = store.StatusFailedPermanent
		o.LastError = lastError(name, res)
	default: // Temporary, RateLimited
		o.LastError = lastError(name, res)
		if attempt >= d.Backoff.MaxAttempts {
			o.NewStatus = store.StatusFailedExhausted
			break
		}
		wait, calc := d.Backoff.Next(attempt, res.RetryAfter)
		due = time.Now().Add(wait)
		o.NewStatus, o.NextAttempt = store.StatusRetryScheduled, &due
		ev["backoff"] = calc
		ev["next_attempt_at"] = due.UTC()
	}
	d.Log.Info("attempt", "id", n.ID, "attempt", attempt, "kind", res.Kind, "status", res.HTTPStatus, "next", o.NewStatus)
	observe(n, name, res, o.NewStatus)
	// A fresh context: once the provider has answered, the answer must be
	// written down even if shutdown has started.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := d.Store.Finish(wctx, n.ID, o, ev); err != nil {
		return err
	}
	if o.NewStatus == store.StatusRetryScheduled {
		d.Delay.Push(delayq.Item{ID: n.ID, Due: due, Kind: "retry", Attempts: attempt})
		top, _ := d.Delay.Peek()
		_ = d.Store.AddEvent(wctx, n.ID, "delayed", "", "", map[string]any{"next_attempt_at": due.UTC(), "heap_len": d.Delay.Len(), "heap_top": top.ID, "heap_top_due": top.Due.UTC(), "worker": d.Name})
	}
	return nil
}

// @learn:end schedule-retry

// RunScheduler pops due items from the delay queue until ctx ends.
func (d *Deliverer) RunScheduler(ctx context.Context) {
	d.Delay.Run(ctx, func(it delayq.Item) {
		go func() { // never block the queue's goroutine
			d.sem <- struct{}{}
			defer func() { <-d.sem }()
			d.fire(ctx, it)
		}()
	})
}

func (d *Deliverer) fire(ctx context.Context, it delayq.Item) {
	now := time.Now()
	if it.Kind == "lease" {
		ok, err := d.Store.ExpireLease(ctx, store.Leased{ID: it.ID, Attempts: it.Attempts, LeaseUntil: it.Due}, store.StatusRetryScheduled, &now)
		if err != nil || !ok {
			return // the attempt finished after all, or someone else took it
		}
		d.Log.Warn("lease expired; retrying", "id", it.ID, "attempt", it.Attempts)
		metrics.LeaseExpired.Inc()
	} else {
		_ = d.Store.AddEvent(ctx, it.ID, "woken", "", "", map[string]any{"due": it.Due.UTC(), "late_ms": float64(now.Sub(it.Due)) / float64(time.Millisecond),
			"timer_fires": d.Delay.TimerFires.Load(), "kicks": d.Delay.Kicks.Load(), "worker": d.Name})
	}
	if _, err := d.Deliver(context.WithoutCancel(ctx), it.ID); err != nil {
		d.Log.Error("retry", "id", it.ID, "err", err)
	}
}

// Reload puts every pending retry, and every attempt left in SENDING
// (with its lease expiry), into the delay queue. It runs at startup and
// whenever this worker is assigned partitions (a peer may have died and
// left retries that only lived in its memory).
// @learn:reload
func (d *Deliverer) Reload(ctx context.Context) (int, error) {
	sched, err := d.Store.Scheduled(ctx)
	if err != nil {
		return 0, err
	}
	for _, l := range sched {
		d.Delay.Push(delayq.Item{ID: l.ID, Due: l.LeaseUntil, Kind: "retry", Attempts: l.Attempts})
	}
	inflight, err := d.Store.InFlight(ctx)
	if err != nil {
		return 0, err
	}
	for _, l := range inflight {
		d.Delay.Push(delayq.Item{ID: l.ID, Due: l.LeaseUntil, Kind: "lease", Attempts: l.Attempts})
	}
	return len(sched) + len(inflight), nil
}

// @learn:end reload

// observe updates the delivery metrics for one attempt.
// @learn:observe
func observe(n *store.Notification, name string, res provider.Result, status string) {
	metrics.Attempts.WithLabelValues(name, string(res.Kind)).Inc()
	if res.Latency > 0 {
		metrics.ProviderSeconds.WithLabelValues(name).Observe(res.Latency.Seconds())
	}
	if res.Kind != provider.Delivered {
		metrics.ProviderErrors.WithLabelValues(name, string(res.Kind), res.Code).Inc()
	}
	switch status {
	case store.StatusDelivered:
		metrics.DeliveryLatency.WithLabelValues(n.Channel).Observe(metrics.Since(n.CreatedAt))
		metrics.Final.WithLabelValues(n.Channel, status).Inc()
	case store.StatusFailedPermanent, store.StatusFailedExhausted:
		metrics.Final.WithLabelValues(n.Channel, status).Inc()
	case store.StatusRetryScheduled:
		metrics.RetriesScheduled.WithLabelValues(name).Inc()
	}
}

// @learn:end observe

func lastError(name string, r provider.Result) map[string]any {
	return map[string]any{"provider": name, "kind": r.Kind, "http_status": r.HTTPStatus, "code": r.Code, "message": r.Message, "action": r.Action, "raw": r.Body}
}
