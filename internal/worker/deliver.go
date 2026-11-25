// Package worker delivers notifications: claim an attempt in MySQL, send
// it to the provider, record the result.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// Deliverer runs one attempt at a time for a notification.
type Deliverer struct {
	Store     *store.Store
	Providers map[string]*provider.Client // by channel
	Log       *slog.Logger
}

// Deliver makes the next attempt for a notification, if it still needs one.
// It returns skipped=true when another worker (or an earlier copy of the
// same message) already owns that attempt.
func (d *Deliverer) Deliver(ctx context.Context, id string) (skipped bool, err error) {
	n, err := d.Store.Notification(ctx, "", id)
	if err != nil {
		return false, err
	}
	if n.Status != store.StatusQueued {
		return true, nil
	}
	client := d.Providers[n.Channel]
	ok, err := d.Store.Claim(ctx, id, n.Attempts, client.Name)
	if err != nil || !ok {
		return !ok, err
	}
	attempt := n.Attempts + 1
	var params map[string]any
	_ = json.Unmarshal(n.Params, &params)
	res := client.Send(ctx, provider.Message{NotificationID: id, Attempt: attempt, Recipient: n.Recipient, Template: n.Template, Params: params})
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
	return false, d.Store.Finish(ctx, id, o, map[string]any{"provider_response": res})
}

func lastError(name string, r provider.Result) map[string]any {
	return map[string]any{"provider": name, "kind": r.Kind, "http_status": r.HTTPStatus, "code": r.Code, "message": r.Message, "action": r.Action, "raw": r.Body}
}
