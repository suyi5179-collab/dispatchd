package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// ConsumerClient joins the worker group. Offsets are committed by hand,
// only after the outcome is in MySQL (at-least-once).
// onAssigned runs (briefly) whenever the group hands this worker partitions.
func ConsumerClient(brokers []string, group string, topics []string, session time.Duration, onAssigned func()) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {
			if onAssigned != nil {
				onAssigned()
			}
		}),
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(session),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
}

// Consumer feeds Kafka records to the Deliverer.
type Consumer struct {
	Client      *kgo.Client
	D           *Deliverer
	Log         *slog.Logger
	Concurrency int
	Name        string // worker name, written into events
}

// Run polls, delivers each record, and commits only after the whole
// batch is recorded. If the process dies in the middle, the batch is
// read again by whoever gets the partitions, and Deliver skips what is
// already done: Kafka gives at-least-once, the claim in MySQL turns it
// into one attempt per (notification, attempt number).
// @learn:consume
func (c *Consumer) Run(ctx context.Context) error {
	work := context.WithoutCancel(ctx) // finish the batch on shutdown
	// BlockRebalanceOnPoll: a rebalance waits until we say the polled
	// records are done, so we never commit offsets for partitions that
	// were already handed to another worker. Every exit must release it.
	defer c.Client.AllowRebalance()
	for {
		fetches := c.Client.PollRecords(ctx, 256)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) { c.Log.Warn("fetch", "topic", t, "partition", p, "err", err) })
		recs := fetches.Records()
		if err := c.handleAll(work, recs); err != nil {
			return err // exit without committing; a restart reads these again
		}
		if len(recs) > 0 {
			if err := c.Client.CommitRecords(work, recs...); err != nil {
				c.Log.Warn("commit", "err", err)
			}
		}
		c.Client.AllowRebalance()
	}
}

// @learn:end consume

func (c *Consumer) handleAll(ctx context.Context, recs []*kgo.Record) error {
	n := max(c.Concurrency, 1)
	sem := make(chan struct{}, n)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, r := range recs {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			var err error
			for try := 0; try < 5; try++ {
				if err = c.handle(ctx, r); err == nil {
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
			mu.Lock()
			if firstErr == nil {
				firstErr = fmt.Errorf("record %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return firstErr
}

func (c *Consumer) handle(ctx context.Context, r *kgo.Record) error {
	var m store.Message
	if err := json.Unmarshal(r.Value, &m); err != nil || m.NotificationID == "" {
		c.Log.Error("bad message; skipping", "topic", r.Topic, "offset", r.Offset)
		return nil
	}
	where := map[string]any{"topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "worker": c.Name}
	if err := c.D.Store.AddEvent(ctx, m.NotificationID, "consumed", "", "", where); err != nil {
		return err
	}
	skipped, err := c.D.Deliver(ctx, m.NotificationID)
	if err != nil {
		return err
	}
	if skipped {
		if n, err := c.D.Store.Notification(ctx, "", m.NotificationID); err == nil {
			where["status"] = n.Status
			where["attempts"] = n.Attempts
		}
		Duplicates.Add(1)
		return c.D.Store.AddEvent(ctx, m.NotificationID, "duplicate_skipped", "", "", where)
	}
	return nil
}
