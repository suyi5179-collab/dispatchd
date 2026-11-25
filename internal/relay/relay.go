// Package relay moves outbox rows into Kafka.
package relay

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/suyi5179-collab/dispatchd/internal/metrics"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// Relay publishes the outbox. The API wakes it right after each commit,
// so the normal path has no polling delay; a slow sweep every Interval
// catches rows left behind by a crash or a Kafka outage.
type Relay struct {
	Store    *store.Store
	Kafka    *kgo.Client
	Log      *slog.Logger
	Interval time.Duration
	wake     chan struct{}
}

// New makes a relay.
func New(st *store.Store, kc *kgo.Client, log *slog.Logger) *Relay {
	return &Relay{Store: st, Kafka: kc, Log: log, Interval: time.Second, wake: make(chan struct{}, 1)}
}

// Wake asks for a publish pass soon (never blocks).
func (r *Relay) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run publishes until ctx ends.
func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		for {
			n, err := r.Store.PublishBatch(ctx, 200, r.send)
			if err != nil {
				r.Log.Warn("relay", "err", err)
				break
			}
			metrics.Published.Add(float64(n))
			if n == 0 {
				break
			}
		}
		if u, err := r.Store.Unpublished(ctx); err == nil {
			metrics.OutboxUnpublished.Set(float64(u))
		}
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		case <-t.C:
		}
	}
}

func (r *Relay) send(rows []store.OutboxRow) ([]store.Published, error) {
	recs := make([]*kgo.Record, len(rows))
	for i, row := range rows {
		recs[i] = &kgo.Record{Topic: row.Topic, Key: []byte(row.NotificationID), Value: row.Payload,
			Headers: []kgo.RecordHeader{{Key: "outbox-id", Value: []byte(strconv.FormatInt(row.ID, 10))}}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := r.Kafka.ProduceSync(ctx, recs...)
	var out []store.Published
	var firstErr error
	for i, pr := range res {
		if pr.Err != nil {
			if firstErr == nil {
				firstErr = pr.Err
			}
			continue
		}
		out = append(out, store.Published{ID: rows[i].ID, Partition: pr.Record.Partition, Offset: pr.Record.Offset})
	}
	return out, firstErr
}
