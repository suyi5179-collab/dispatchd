// Package api is the HTTP front door: callers submit notifications and
// read back their delivery state.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/queue"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// Server holds what the handlers need.
type Server struct {
	Store *store.Store
	Log   *slog.Logger
	// Enqueue is called after a new notification is committed. With the
	// outbox it only wakes the relay; the message itself is already safe
	// in the outbox table.
	Enqueue func(ctx context.Context, id string)
	// Topics names the Kafka topic for each channel.
	Topics queue.Topics
}

type ctxKey struct{}

// ClientFrom returns the authenticated caller.
func ClientFrom(ctx context.Context) store.Client {
	c, _ := ctx.Value(ctxKey{}).(store.Client)
	return c
}

// JSON writes a JSON response.
func JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Error writes {"error": msg}.
func Error(w http.ResponseWriter, code int, msg string) {
	JSON(w, code, map[string]string{"error": msg})
}

// auth accepts "Authorization: Bearer <key>" or "X-API-Key: <key>".
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if key == "" {
			key = r.Header.Get("X-API-Key")
		}
		if key == "" {
			Error(w, 401, "missing API key")
			return
		}
		c, err := s.Store.ClientByKey(r.Context(), key)
		if err != nil {
			Error(w, 401, "invalid API key")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, c)))
	}
}

// Request is the body of POST /v1/notifications.
type Request struct {
	Channel   string         `json:"channel"`
	Recipient string         `json:"recipient"`
	Template  string         `json:"template"`
	Params    map[string]any `json:"params"`
}

func (q *Request) validate() error {
	switch q.Channel {
	case "email", "push":
	default:
		return errors.New(`channel must be "email" or "push"`)
	}
	if q.Recipient == "" || len(q.Recipient) > 320 {
		return errors.New("recipient is required (at most 320 characters)")
	}
	if _, ok := provider.Templates[q.Template]; !ok {
		return errors.New("unknown template " + strconv.Quote(q.Template))
	}
	if q.Params == nil {
		q.Params = map[string]any{}
	}
	return nil
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	c := ClientFrom(r.Context())
	n, err := s.Store.Notification(r.Context(), c.ID, r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		Error(w, 404, "no such notification")
		return
	}
	if err != nil {
		Error(w, 500, err.Error())
		return
	}
	attempts, _ := s.Store.Attempts(r.Context(), n.ID)
	events, _ := s.Store.Events(r.Context(), n.ID)
	JSON(w, 200, map[string]any{"notification": n, "attempts": attempts, "events": events})
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	ns, err := s.Store.List(r.Context(), ClientFrom(r.Context()).ID, store.Filter{Status: q.Get("status"), Channel: q.Get("channel"), Limit: limit})
	if err != nil {
		Error(w, 500, err.Error())
		return
	}
	JSON(w, 200, map[string]any{"notifications": ns})
}

// Routes returns the HTTP handler.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { JSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /v1/notifications", s.auth(s.create))
	mux.HandleFunc("GET /v1/notifications/{id}", s.auth(s.get))
	mux.HandleFunc("GET /v1/notifications", s.auth(s.list))
	return mux
}

// afterInsert runs inside the create transaction: the outbox row commits
// together with the notification, or neither does.
// @learn:outbox-write
func (s *Server) afterInsert(ctx context.Context, tx *sql.Tx, n *store.Notification) error {
	return store.InsertOutbox(ctx, tx, s.Topics.For(n.Channel), store.Message{NotificationID: n.ID, Channel: n.Channel, CreatedAt: n.CreatedAt})
}

// @learn:end outbox-write

// replayed counts idempotent replays (a metric from chapter 06).
func (s *Server) replayed() {}

// ExpireKeys deletes idempotency keys older than ttl, every interval.
func (s *Server) ExpireKeys(ctx context.Context, ttl, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.Store.ExpireIdem(ctx, ttl); err == nil && n > 0 {
				s.Log.Info("expired idempotency keys", "n", n)
			}
		}
	}
}
