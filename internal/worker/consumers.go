package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/queue"
)

// Channels the service delivers to, one topic each.
var Channels = []string{"email", "push"}

// GroupFor is the consumer group of one channel.
func GroupFor(base, channel string) string { return base + "." + channel }

// NewConsumers makes one consumer per channel, each with its own Kafka
// client and its own consumer group. A poll loop waits for its whole
// batch before polling again, so if one loop read both topics, push
// records would wait behind email records that are waiting for email
// tokens. Separate loops keep a slow or throttled channel to itself.
// @learn:per-channel-consumers
func NewConsumers(brokers []string, groupBase string, topics queue.Topics, session time.Duration, d *Deliverer, log *slog.Logger, concurrency int, name string, onAssigned func()) ([]*Consumer, error) {
	var out []*Consumer
	for _, ch := range Channels {
		kc, err := ConsumerClient(brokers, GroupFor(groupBase, ch), []string{topics.For(ch)}, session, onAssigned)
		if err != nil {
			CloseAll(out)
			return nil, err
		}
		out = append(out, &Consumer{Client: kc, D: d, Log: log.With("channel", ch), Concurrency: concurrency, Name: name})
	}
	return out, nil
}

// @learn:end per-channel-consumers

// RunAll runs every consumer and returns when ctx ends (nil) or the
// first one fails (its error; the caller exits and a restart re-reads
// whatever was not committed).
func RunAll(ctx context.Context, cs []*Consumer) error {
	errs := make(chan error, len(cs))
	for _, c := range cs {
		go func() { errs <- c.Run(ctx) }()
	}
	var first error
	for range cs {
		if err := <-errs; err != nil && first == nil {
			first = err
			return first
		}
	}
	return first
}

// CloseAll closes the consumers' Kafka clients.
func CloseAll(cs []*Consumer) {
	for _, c := range cs {
		c.Client.Close()
	}
}
