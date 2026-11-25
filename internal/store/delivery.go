package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// claimable are the statuses a worker may start an attempt from.
var claimable = []any{StatusQueued}

// Claim moves a notification into SENDING for attempt prev+1, but only if
// it is still where the caller saw it (status claimable and attempts ==
// prev). Two workers holding the same message both try; exactly one
// UPDATE matches, so the other learns it lost and must not send. This is
// the worker-side dedup: it is keyed on (notification id, attempt).
// @learn:claim
func (s *Store) Claim(ctx context.Context, id string, prev int, provider string, lease time.Duration) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	var from string
	err = tx.QueryRowContext(ctx, `SELECT status FROM notifications WHERE id=? AND attempts=? FOR UPDATE`, id, prev).Scan(&from)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ok := false
	for _, c := range claimable {
		if from == c {
			ok = true
		}
	}
	if !ok {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE notifications SET status=?, attempts=?, lease_until=?, updated_at=? WHERE id=?`, StatusSending, prev+1, now.Add(lease), now, id); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_attempts (notification_id, attempt, provider, started_at, outcome) VALUES (?,?,?,?,?)`,
		id, prev+1, provider, now, "IN_FLIGHT"); err != nil {
		return false, err
	}
	if err := AddEvent(ctx, tx, id, "claimed", from, StatusSending, map[string]any{"attempt": prev + 1, "provider": provider}); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// @learn:end claim

// Outcome is what one attempt ended with.
type Outcome struct {
	Attempt      int
	Outcome      string // DELIVERED, TEMPORARY, RATE_LIMITED, PERMANENT
	HTTPStatus   int
	ProviderCode string
	Detail       any
	LatencyMS    float64
	NewStatus    string
	LastError    any // nil on success
}

// Finish records an attempt's result and the notification's next status
// in one transaction, plus a status-transition event.
func (s *Store) Finish(ctx context.Context, id string, o Outcome, eventData map[string]any) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_attempts SET finished_at=?, outcome=?, http_status=?, provider_code=?, detail=?, latency_ms=? WHERE notification_id=? AND attempt=?`,
		now, o.Outcome, o.HTTPStatus, o.ProviderCode, jsonOrNull(o.Detail), o.LatencyMS, id, o.Attempt); err != nil {
		return err
	}
	var delivered any
	if o.NewStatus == StatusDelivered {
		delivered = now
	}
	if _, err := tx.ExecContext(ctx, `UPDATE notifications SET status=?, last_error=?, lease_until=NULL, updated_at=?, delivered_at=COALESCE(?, delivered_at) WHERE id=? AND attempts=? AND status=?`,
		o.NewStatus, jsonOrNull(o.LastError), now, delivered, id, o.Attempt, StatusSending); err != nil {
		return err
	}
	if eventData == nil {
		eventData = map[string]any{}
	}
	eventData["attempt"] = o.Attempt
	eventData["outcome"] = o.Outcome
	eventData["http_status"] = o.HTTPStatus
	if err := AddEvent(ctx, tx, id, "attempt", StatusSending, o.NewStatus, eventData); err != nil {
		return err
	}
	return tx.Commit()
}

// RawJSON marshals v for an event payload.
func RawJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// Leased is a notification some worker started an attempt on.
type Leased struct {
	ID         string
	Attempts   int
	LeaseUntil time.Time
}

// InFlight lists notifications in SENDING: attempts that some worker
// started. After a crash these are the ones nobody will finish.
func (s *Store) InFlight(ctx context.Context) ([]Leased, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, attempts, COALESCE(lease_until, updated_at) FROM notifications WHERE status=?`, StatusSending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Leased
	for rows.Next() {
		var l Leased
		if err := rows.Scan(&l.ID, &l.Attempts, &l.LeaseUntil); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ExpireLease gives up on an attempt whose worker stopped renewing it
// (it crashed): the attempt is marked UNKNOWN, because the provider may
// or may not have received it, and the notification goes back to
// toStatus. It only matches if the lease really ran out and nobody moved
// the row since.
// @learn:expire-lease
func (s *Store) ExpireLease(ctx context.Context, l Leased, toStatus string, nextAt *time.Time) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE notifications SET status=?, lease_until=NULL, next_attempt_at=?, updated_at=? WHERE id=? AND status=? AND attempts=? AND lease_until<=?`,
		toStatus, nextAt, now, l.ID, StatusSending, l.Attempts, now)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_attempts SET outcome='UNKNOWN', finished_at=? WHERE notification_id=? AND attempt=? AND outcome='IN_FLIGHT'`, now, l.ID, l.Attempts); err != nil {
		return false, err
	}
	if err := AddEvent(ctx, tx, l.ID, "lease_expired", StatusSending, toStatus, map[string]any{"attempt": l.Attempts, "lease_until": l.LeaseUntil,
		"why": "the worker holding this attempt stopped (crash or kill); the provider may or may not have received it, so it is sent again with the same provider idempotency key"}); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// @learn:end expire-lease
