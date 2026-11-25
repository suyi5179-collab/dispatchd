//go:build ignore

package worker

// The consumer loop before the fix: with kgo.BlockRebalanceOnPoll, a
// rebalance (and Close, which leaves the group) waits until the polled
// records are released by AllowRebalance. The ctx-done return skipped it.
// @learn:consume-v1
func (c *Consumer) Run(ctx context.Context) error {
	work := context.WithoutCancel(ctx)
	for {
		fetches := c.Client.PollRecords(ctx, 256)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil // still holding the poll: Close() now blocks forever
		}
		recs := fetches.Records()
		if err := c.handleAll(work, recs); err != nil {
			return err // same here
		}
		_ = c.Client.CommitRecords(work, recs...)
		c.Client.AllowRebalance()
	}
}

// @learn:end consume-v1
