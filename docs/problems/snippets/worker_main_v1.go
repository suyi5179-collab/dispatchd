//go:build ignore

package main

// cmd/worker before the fix: one Kafka client, one consumer group, both
// topics. A poll returns email and push records together, and the next
// poll only happens after the whole batch is done, so push records wait
// for email records that are waiting for email tokens.
func main() {
	// @learn:one-consumer-v1
	kc, err := worker.ConsumerClient(brokers, config.Env("DISPATCHD_GROUP", "dispatchd-workers"), topics.All(), session, onAssigned)
	c := &worker.Consumer{Client: kc, D: d, Log: log, Concurrency: 32, Name: name}
	if err := c.Run(ctx); err != nil {
		os.Exit(1)
	}
	// @learn:end one-consumer-v1
}
