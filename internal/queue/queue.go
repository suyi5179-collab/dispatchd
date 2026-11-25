// Package queue holds the Kafka plumbing: topic names, client setup and
// topic creation.
package queue

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Topics maps a channel to its topic. One topic per channel, keyed by
// notification id: an email outage backs up the email topic only, and the
// push workers keep going.
type Topics struct{ Prefix string }

// For returns the topic of a channel.
func (t Topics) For(channel string) string { return t.Prefix + "notifications." + channel }

// All topics.
func (t Topics) All() []string { return []string{t.For("email"), t.For("push")} }

// Brokers splits a comma-separated list.
func Brokers(s string) []string { return strings.Split(s, ",") }

// Producer makes a producing client (acks=all, idempotent producer).
func Producer(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ProducerLinger(2*time.Millisecond), kgo.RequiredAcks(kgo.AllISRAcks()))
}

// EnsureTopics creates the topics if they do not exist yet.
func EnsureTopics(ctx context.Context, brokers []string, partitions int32, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	var last error
	for i := 0; i < 60; i++ {
		res, err := adm.CreateTopics(ctx, partitions, 1, nil, topics...)
		if err == nil {
			last = nil
			for _, r := range res {
				if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
					last = r.Err
				}
			}
			if last == nil {
				return nil
			}
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return last
}

// DeleteTopics removes topics (tests clean up after themselves).
func DeleteTopics(ctx context.Context, brokers []string, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	_, err = kadm.NewClient(cl).DeleteTopics(ctx, topics...)
	return err
}
