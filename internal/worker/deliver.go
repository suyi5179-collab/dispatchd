// Package worker delivers notifications: claim an attempt in MySQL, send
// it to the provider, record the result.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// Deliverer runs one attempt at a time for a notification.
type Deliverer struct {
	Store     *store.Store
	Providers map[string]*provider.Client // by channel
	Log       *slog.Logger
	// Lease is how long a claimed attempt belongs to this worker. It must
	// be longer than the provider timeout.
	Lease time.Duration
}

func (d *Deliverer) lease() time.Duration {
	if d.Lease > 0 {
		return d.Lease
	}
	return 15 * time.Second
}

// Deliver makes the next attempt for a notification, if it still needs one.
// It returns skipped=true when another worker (or an earlier copy of the
// same Kafka message) already owns that attempt or the notification is done.
// @learn:deliver
func (d *Deliverer) Deliver(ctx context.Context, id string) (skipped bool, err error) {
	n, err := d.Store.Notification(ctx, "", id)
	if err != nil {
		return false, err
	}
	if n.Status != store.StatusQueued {
		return true, nil
	}
	client := d.Providers[n.Channel]
	ok, err := d.Store.Claim(ctx, id, n.Attempts, client.Name, d.lease())
	if err != nil || !ok {
		return !ok, err
	}
	attempt := n.Attempts + 1
	var params map[string]any
	_ = json.Unmarshal(n.Params, &params)
	res := client.Send(ctx, provider.Message{NotificationID: id, Attempt: attempt, Recipient: n.Recipient, Template: n.Template, Params: params})
	// @learn:end deliver
	o := store.Outcome{Attempt: attempt, Outcome: string(res.Kind), HTTPStatus: res.HTTPStatus, ProviderCode: res.Code, Detail: res, LatencyMS: float64(res.Latency) / float64(time.Millisecond)}
	switch res.Kind {
	case provider.Delivered:
		o.NewStatus = store.StatusDelivered
	case provider.Permanent:
		o.NewStatus = store.StatusFailedPermanent
		o.LastError = lastError(client.Name, res)
	default:
		o.NewStatus = store.StatusFailedTemporary // retries arrive in chapter 04
		o.LastError = lastError(client.Name, res)
	}
	d.Log.Info("attempt", "id", id, "attempt", attempt, "kind", res.Kind, "status", res.HTTPStatus)
	// A fresh context: once the provider has answered, the answer must be
	// written down even if shutdown has started.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return false, d.Store.Finish(wctx, id, o, map[string]any{"provider_response": res})
}

// Recover takes the attempts that were in SENDING when this worker
// started (a dead worker left them there), waits for each lease to run
// out, then puts the notification back in the queue and delivers it.
// Attempts this process starts later are not in the snapshot.
func (d *Deliverer) Recover(ctx context.Context, redeliver func(id string)) error {
	inflight, err := d.Store.InFlight(ctx)
	if err != nil {
		return err
	}
	sort.Slice(inflight, func(i, j int) bool { return inflight[i].LeaseUntil.Before(inflight[j].LeaseUntil) })
	for _, l := range inflight {
		if wait := time.Until(l.LeaseUntil); wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait + 10*time.Millisecond):
			}
		}
		if ok, err := d.Store.ExpireLease(ctx, l, store.StatusQueued, nil); err == nil && ok {
			d.Log.Warn("lease expired; redelivering", "id", l.ID, "attempt", l.Attempts)
			redeliver(l.ID)
		}
	}
	return nil
}

func lastError(name string, r provider.Result) map[string]any {
	return map[string]any{"provider": name, "kind": r.Kind, "http_status": r.HTTPStatus, "code": r.Code, "message": r.Message, "action": r.Action, "raw": r.Body}
}
