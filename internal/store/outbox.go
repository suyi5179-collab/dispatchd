package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// OutboxRow is one message waiting to be (or already) published.
type OutboxRow struct {
	ID             int64           `json:"id"`
	NotificationID string          `json:"notification_id"`
	Topic          string          `json:"topic"`
	Payload        json.RawMessage `json:"payload"`
	CreatedAt      time.Time       `json:"created_at"`
	PublishedAt    *time.Time      `json:"published_at,omitempty"`
	Partition      *int32          `json:"kafka_partition,omitempty"`
	Offset         *int64          `json:"kafka_offset,omitempty"`
}

// Message is the Kafka message body.
type Message struct {
	NotificationID string    `json:"notification_id"`
	Channel        string    `json:"channel"`
	CreatedAt      time.Time `json:"created_at"`
}

// InsertOutbox adds a row inside the caller's transaction.
func InsertOutbox(ctx context.Context, x Execer, topic string, m Message) error {
	b, _ := json.Marshal(m)
	_, err := x.ExecContext(ctx, `INSERT INTO outbox (notification_id, topic, payload, created_at) VALUES (?,?,?,?)`, m.NotificationID, topic, string(b), time.Now().UTC())
	return err
}

// Published is what the relay learned from Kafka for one row.
type Published struct {
	ID        int64
	Partition int32
	Offset    int64
}

// PublishBatch locks up to limit unpublished rows (SKIP LOCKED, so two
// relays split the work instead of sending the same rows), hands them to
// send, and marks the ones send reports as published, all in one
// transaction. If the process dies after Kafka accepted a message but
// before the commit, the row is sent again: the relay is at-least-once and
// the worker dedups.
// @learn:relay-batch
func (s *Store) PublishBatch(ctx context.Context, limit int, send func([]OutboxRow) ([]Published, error)) (int, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id, notification_id, topic, payload, created_at FROM outbox
		WHERE published_at IS NULL ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var batch []OutboxRow
	for rows.Next() {
		var r OutboxRow
		var p []byte
		if err := rows.Scan(&r.ID, &r.NotificationID, &r.Topic, &p, &r.CreatedAt); err != nil {
			rows.Close()
			return 0, err
		}
		r.Payload = p
		batch = append(batch, r)
	}
	rows.Close()
	if len(batch) == 0 {
		return 0, nil
	}
	done, err := send(batch)
	now := time.Now().UTC()
	byID := map[int64]OutboxRow{}
	for _, r := range batch {
		byID[r.ID] = r
	}
	for _, p := range done {
		if _, err := tx.ExecContext(ctx, `UPDATE outbox SET published_at=?, kafka_partition=?, kafka_offset=? WHERE id=?`, now, p.Partition, p.Offset, p.ID); err != nil {
			return 0, err
		}
		r := byID[p.ID]
		if err := AddEvent(ctx, tx, r.NotificationID, "published", "", "", map[string]any{
			"topic": r.Topic, "partition": p.Partition, "offset": p.Offset, "key": r.NotificationID, "payload": r.Payload, "outbox_id": r.ID}); err != nil {
			return 0, err
		}
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, cerr
	}
	return len(done), err
}

// @learn:end relay-batch

// OutboxFor returns a notification's outbox rows.
func (s *Store) OutboxFor(ctx context.Context, id string) ([]OutboxRow, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, notification_id, topic, payload, created_at, published_at, kafka_partition, kafka_offset FROM outbox WHERE notification_id=? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []OutboxRow{}
	for rows.Next() {
		var r OutboxRow
		var p []byte
		if err := rows.Scan(&r.ID, &r.NotificationID, &r.Topic, &p, &r.CreatedAt, &r.PublishedAt, &r.Partition, &r.Offset); err != nil {
			return nil, err
		}
		r.Payload = p
		out = append(out, r)
	}
	return out, rows.Err()
}

// Unpublished counts outbox rows not yet in Kafka.
func (s *Store) Unpublished(ctx context.Context) (int, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return n, err
}
