package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/suyi5179-collab/dispatchd/internal/metrics"
)

// Lag reports the group's consumer lag per topic: messages written to
// Kafka that the workers have not committed yet. It is the queue depth.
func Lag(ctx context.Context, brokers []string, group string, topics []string) (map[string]int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	lags, err := kadm.NewClient(cl).Lag(ctx, group)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, t := range topics {
		out[t] = 0
	}
	if l, ok := lags[group]; ok {
		if err := l.Error(); err != nil {
			return nil, err
		}
		for t, tl := range l.Lag.TotalByTopic() {
			if _, want := out[t]; want {
				out[t] = tl.Lag
			}
		}
	}
	return out, nil
}

// WatchLag updates dispatchd_kafka_consumer_lag every interval.
func WatchLag(ctx context.Context, log *slog.Logger, brokers []string, group string, topics []string, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if lag, err := Lag(ctx, brokers, group, topics); err == nil {
			for topic, n := range lag {
				metrics.KafkaLag.WithLabelValues(topic).Set(float64(n))
			}
		} else if ctx.Err() == nil {
			log.Warn("lag", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
