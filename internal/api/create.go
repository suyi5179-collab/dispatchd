package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/config"
	"github.com/suyi5179-collab/dispatchd/internal/ids"
	"github.com/suyi5179-collab/dispatchd/internal/metrics"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// Fingerprint identifies a request body regardless of key order, so the
// same request sent twice hashes the same (json.Marshal sorts map keys).
func Fingerprint(q Request) string {
	b, _ := json.Marshal(q)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// errKeyReuse is a key sent again with a different body.
var errKeyReuse = errors.New("Idempotency-Key was already used with a different request body")

type created struct {
	code     int
	body     json.RawMessage
	id       string
	replayed bool
}

// create handles POST /v1/notifications. The Idempotency-Key header is
// required: a caller that timed out and retries sends the same key, and
// gets the first notification back instead of creating a second one.
func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req Request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		Error(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if err := req.validate(); err != nil {
		Error(w, 400, err.Error())
		return
	}
	key := r.Header.Get("Idempotency-Key")
	// @learn:idem-sabotage
	if config.Sabotaged(config.NoIdempotency) {
		key = "" // sabotage: behave as if the service never had keys
		// @learn:end idem-sabotage
	} else if key == "" || len(key) > 255 {
		Error(w, 400, "Idempotency-Key header is required (at most 255 characters)")
		return
	}
	c := ClientFrom(r.Context())
	var res created
	var err error
	for try := 0; try < 3; try++ {
		if res, err = s.createOnce(r.Context(), c, req, key); !store.IsDeadlock(err) {
			break
		}
	}
	switch {
	case errors.Is(err, errKeyReuse):
		metrics.KeyConflicts.Inc()
		JSON(w, 422, map[string]any{"error": err.Error(), "notification_id": res.id})
		return
	case err != nil:
		s.Log.Error("create", "err", err)
		Error(w, 500, "could not store the notification")
		return
	}
	if res.replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		_ = s.Store.AddEvent(context.WithoutCancel(r.Context()), res.id, "replayed", "", "", map[string]any{"idempotency_key": key})
		s.replayed()
	} else {
		metrics.Accepted.WithLabelValues(req.Channel).Inc()
		s.Enqueue(context.WithoutCancel(r.Context()), res.id)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(res.code)
	_, _ = w.Write(res.body)
}

// createOnce stores a new notification and its key in one transaction.
// @learn:idempotency
func (s *Server) createOnce(ctx context.Context, c store.Client, req Request, key string) (created, error) {
	params, _ := json.Marshal(req.Params)
	now := time.Now().UTC()
	n := &store.Notification{ID: ids.New("ntf_"), ClientID: c.ID, Channel: req.Channel, Recipient: req.Recipient, Template: req.Template,
		Params: params, Status: store.StatusQueued, CreatedAt: now, UpdatedAt: now}
	body, _ := json.Marshal(n)
	hash := Fingerprint(req)

	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return created{}, err
	}
	defer tx.Rollback()
	if key != "" {
		// No SELECT first: two concurrent requests would both see "no
		// row" and both insert a notification. The primary key on
		// (client_id, idem_key) lets exactly one INSERT win; the other
		// waits for it to commit and then gets a duplicate-key error.
		err := store.InsertIdem(ctx, tx, store.IdemRecord{ClientID: c.ID, Key: key, RequestHash: hash, NotificationID: n.ID, StatusCode: 202, Response: body, CreatedAt: now})
		if store.IsDuplicateKey(err) {
			_ = tx.Rollback()
			prev, err := s.Store.Idem(ctx, c.ID, key)
			if err != nil {
				return created{}, err
			}
			if prev.RequestHash != hash {
				return created{id: prev.NotificationID}, errKeyReuse
			}
			return created{code: prev.StatusCode, body: prev.Response, id: prev.NotificationID, replayed: true}, nil
		}
		if err != nil {
			return created{}, err
		}
	}
	if err := store.InsertNotification(ctx, tx, n); err != nil {
		return created{}, err
	}
	if err := store.AddEvent(ctx, tx, n.ID, "accepted", "", store.StatusQueued, map[string]any{"request": req, "idempotency_key": key, "request_hash": hash}); err != nil {
		return created{}, err
	}
	if err := s.afterInsert(ctx, tx, n); err != nil {
		return created{}, err
	}
	return created{code: 202, body: body, id: n.ID}, tx.Commit()
}

// @learn:end idempotency
