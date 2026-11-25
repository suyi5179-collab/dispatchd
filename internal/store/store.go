// Package store keeps all delivery state in MySQL: notifications, their
// attempts, an event log, and (in later chapters) idempotency keys and
// the outbox.
package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Notification statuses.
const (
	StatusQueued          = "QUEUED"
	StatusSending         = "SENDING"
	StatusDelivered       = "DELIVERED"
	StatusFailedPermanent = "FAILED_PERMANENT"
	StatusFailedTemporary = "FAILED_TEMPORARY" // step-01 only: no retries yet
)

// ErrNotFound means no such row (or not the caller's).
var ErrNotFound = errors.New("not found")

// Store wraps the database.
type Store struct {
	DB *sql.DB
}

// Open connects and pings. The DSN is a go-sql-driver DSN; parseTime and
// UTC are forced so DATETIME columns round-trip as time.Time.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(16)
	db.SetConnMaxLifetime(5 * time.Minute)
	var last error
	for i := 0; i < 60; i++ {
		if last = db.PingContext(ctx); last == nil {
			return &Store{DB: db}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, fmt.Errorf("mysql not reachable: %w", last)
}

// Migrate applies migrations/*.sql in order, once each.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version VARCHAR(64) PRIMARY KEY, applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))`); err != nil {
		return err
	}
	files, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		var n int
		if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version=?`, name).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		body, _ := migrations.ReadFile("migrations/" + name)
		for _, stmt := range splitSQL(string(body)) {
			if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
		if _, err := s.DB.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (?)`, name); err != nil {
			return err
		}
	}
	return nil
}

func splitSQL(body string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(body, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "--") || t == "" {
			continue
		}
		cur.WriteString(line + "\n")
		if strings.HasSuffix(t, ";") {
			out = append(out, strings.TrimSuffix(strings.TrimSpace(cur.String()), ";"))
			cur.Reset()
		}
	}
	if strings.TrimSpace(cur.String()) != "" {
		out = append(out, cur.String())
	}
	return out
}

// HashKey is how API keys are stored.
func HashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// Client is an API caller.
type Client struct {
	ID   string `json:"id"`
	Role string `json:"role"`
}

// UpsertClient registers an API key (called at startup from config).
func (s *Store) UpsertClient(ctx context.Context, id, key, role string) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO api_clients (id, key_hash, role) VALUES (?,?,?)
		ON DUPLICATE KEY UPDATE key_hash=VALUES(key_hash), role=VALUES(role)`, id, HashKey(key), role)
	return err
}

// ClientByKey resolves an API key.
func (s *Store) ClientByKey(ctx context.Context, key string) (Client, error) {
	var c Client
	err := s.DB.QueryRowContext(ctx, `SELECT id, role FROM api_clients WHERE key_hash=?`, HashKey(key)).Scan(&c.ID, &c.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	return c, err
}

// Notification is one request to notify someone.
type Notification struct {
	ID          string          `json:"id"`
	ClientID    string          `json:"client_id"`
	Channel     string          `json:"channel"`
	Recipient   string          `json:"recipient"`
	Template    string          `json:"template"`
	Params      json.RawMessage `json:"params"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	LastError   json.RawMessage `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	DeliveredAt *time.Time      `json:"delivered_at,omitempty"`
}

// Attempt is one delivery attempt.
type Attempt struct {
	NotificationID string          `json:"-"`
	Attempt        int             `json:"attempt"`
	Provider       string          `json:"provider"`
	StartedAt      time.Time       `json:"started_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	Outcome        string          `json:"outcome"`
	HTTPStatus     int             `json:"http_status"`
	ProviderCode   string          `json:"provider_code,omitempty"`
	Detail         json.RawMessage `json:"detail,omitempty"`
	LatencyMS      float64         `json:"latency_ms"`
}

// Event is one entry of a notification's log.
type Event struct {
	ID    int64           `json:"id"`
	Stage string          `json:"stage"`
	From  string          `json:"from,omitempty"`
	To    string          `json:"to,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	At    time.Time       `json:"at"`
}

// Execer is satisfied by *sql.DB and *sql.Tx.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func jsonOrNull(v any) any {
	if v == nil {
		return nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		if len(raw) == 0 {
			return nil
		}
		return string(raw)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// AddEvent appends to a notification's log.
func AddEvent(ctx context.Context, x Execer, id, stage, from, to string, data any) error {
	_, err := x.ExecContext(ctx, `INSERT INTO notification_events (notification_id, stage, from_status, to_status, data, at) VALUES (?,?,?,?,?,?)`,
		id, stage, from, to, jsonOrNull(data), time.Now().UTC())
	return err
}

// AddEvent on the store's own connection pool.
func (s *Store) AddEvent(ctx context.Context, id, stage, from, to string, data any) error {
	return AddEvent(ctx, s.DB, id, stage, from, to, data)
}

// InsertNotification writes a new notification row.
func InsertNotification(ctx context.Context, x Execer, n *Notification) error {
	_, err := x.ExecContext(ctx, `INSERT INTO notifications (id, client_id, channel, recipient, template, params, status, attempts, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,0,?,?)`, n.ID, n.ClientID, n.Channel, n.Recipient, n.Template, string(n.Params), n.Status, n.CreatedAt, n.CreatedAt)
	return err
}

const notifCols = `id, client_id, channel, recipient, template, params, status, attempts, last_error, created_at, updated_at, delivered_at`

type scanner interface{ Scan(dest ...any) error }

func scanNotification(r scanner) (*Notification, error) {
	var n Notification
	var params, lastErr []byte
	if err := r.Scan(&n.ID, &n.ClientID, &n.Channel, &n.Recipient, &n.Template, &params, &n.Status, &n.Attempts, &lastErr, &n.CreatedAt, &n.UpdatedAt, &n.DeliveredAt); err != nil {
		return nil, err
	}
	n.Params = params
	if len(lastErr) > 0 {
		n.LastError = lastErr
	}
	return &n, nil
}

// Notification loads one notification. clientID "" skips the ownership check
// (workers); callers always pass theirs.
func (s *Store) Notification(ctx context.Context, clientID, id string) (*Notification, error) {
	q := `SELECT ` + notifCols + ` FROM notifications WHERE id=?`
	args := []any{id}
	if clientID != "" {
		q += ` AND client_id=?`
		args = append(args, clientID)
	}
	n, err := scanNotification(s.DB.QueryRowContext(ctx, q, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return n, err
}

// Filter for listing notifications.
type Filter struct {
	Status  string
	Channel string
	Limit   int
}

// List returns a client's most recent notifications.
func (s *Store) List(ctx context.Context, clientID string, f Filter) ([]*Notification, error) {
	q := `SELECT ` + notifCols + ` FROM notifications WHERE client_id=?`
	args := []any{clientID}
	if f.Status != "" {
		q += ` AND status=?`
		args = append(args, f.Status)
	}
	if f.Channel != "" {
		q += ` AND channel=?`
		args = append(args, f.Channel)
	}
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	q += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT %d`, f.Limit)
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Notification{}
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// Attempts lists a notification's attempts in order.
func (s *Store) Attempts(ctx context.Context, id string) ([]Attempt, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT attempt, provider, started_at, finished_at, outcome, http_status, provider_code, detail, latency_ms
		FROM delivery_attempts WHERE notification_id=? ORDER BY attempt`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Attempt{}
	for rows.Next() {
		a := Attempt{NotificationID: id}
		var detail []byte
		if err := rows.Scan(&a.Attempt, &a.Provider, &a.StartedAt, &a.FinishedAt, &a.Outcome, &a.HTTPStatus, &a.ProviderCode, &detail, &a.LatencyMS); err != nil {
			return nil, err
		}
		if len(detail) > 0 {
			a.Detail = detail
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Events lists a notification's log in order.
func (s *Store) Events(ctx context.Context, id string) ([]Event, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, stage, from_status, to_status, data, at FROM notification_events WHERE notification_id=? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		var data []byte
		if err := rows.Scan(&e.ID, &e.Stage, &e.From, &e.To, &data, &e.At); err != nil {
			return nil, err
		}
		if len(data) > 0 {
			e.Data = data
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// IsDuplicateKey reports a unique-constraint violation (MySQL 1062).
func IsDuplicateKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// IsDeadlock reports a deadlock or lock wait timeout (1213 / 1205).
func IsDeadlock(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && (me.Number == 1213 || me.Number == 1205)
}
