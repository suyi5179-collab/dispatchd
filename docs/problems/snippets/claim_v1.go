//go:build ignore

package store

// Claim as first written in chapter 03, when lease_until was added to the
// UPDATE. Five placeholders, four arguments.
// @learn:claim-v1
func (s *Store) Claim(ctx context.Context, id string, prev int, provider string, lease time.Duration) (bool, error) {
	// ... SELECT status ... FOR UPDATE ...
	if _, err := tx.ExecContext(ctx, `UPDATE notifications SET status=?, attempts=?, lease_until=?, updated_at=? WHERE id=?`,
		StatusSending, prev+1, now.Add(lease), id); err != nil {
		return false, err
	}
	// ...
}

// The harness started workers like this: the error that stopped the
// worker went nowhere, and the test only saw "status QUEUED after 15s".
func (s *Stack) StartWorker(name string) {
	go func() {
		defer close(done)
		_ = c.Run(ctx)
	}()
}

// @learn:end claim-v1
