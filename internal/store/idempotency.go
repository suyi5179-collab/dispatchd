package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// IdemRecord is a stored idempotency key.
type IdemRecord struct {
	ClientID       string          `json:"client_id"`
	Key            string          `json:"idem_key"`
	RequestHash    string          `json:"request_hash"`
	NotificationID string          `json:"notification_id"`
	StatusCode     int             `json:"status_code"`
	Response       json.RawMessage `json:"response"`
	CreatedAt      time.Time       `json:"created_at"`
}

// InsertIdem claims a key inside the caller's transaction. A duplicate
// key error (IsDuplicateKey) means someone else already holds it.
func InsertIdem(ctx context.Context, x Execer, r IdemRecord) error {
	_, err := x.ExecContext(ctx, `INSERT INTO idempotency_keys (client_id, idem_key, request_hash, notification_id, status_code, response, created_at) VALUES (?,?,?,?,?,?,?)`,
		r.ClientID, r.Key, r.RequestHash, r.NotificationID, r.StatusCode, string(r.Response), r.CreatedAt)
	return err
}

// Idem loads a stored key.
func (s *Store) Idem(ctx context.Context, clientID, key string) (*IdemRecord, error) {
	r := IdemRecord{ClientID: clientID, Key: key}
	var resp []byte
	err := s.DB.QueryRowContext(ctx, `SELECT request_hash, notification_id, status_code, response, created_at FROM idempotency_keys WHERE client_id=? AND idem_key=?`, clientID, key).
		Scan(&r.RequestHash, &r.NotificationID, &r.StatusCode, &resp, &r.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	r.Response = resp
	return &r, err
}

// IdemForNotification finds the key a notification was created with.
func (s *Store) IdemForNotification(ctx context.Context, id string) (*IdemRecord, error) {
	var client, key string
	err := s.DB.QueryRowContext(ctx, `SELECT client_id, idem_key FROM idempotency_keys WHERE notification_id=?`, id).Scan(&client, &key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.Idem(ctx, client, key)
}

// ExpireIdem deletes keys older than ttl. Callers retry within minutes;
// keeping keys for a day covers that with a wide margin.
func (s *Store) ExpireIdem(ctx context.Context, ttl time.Duration) (int64, error) {
	res, err := s.DB.ExecContext(ctx, `DELETE FROM idempotency_keys WHERE created_at < ? LIMIT 10000`, time.Now().UTC().Add(-ttl))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
