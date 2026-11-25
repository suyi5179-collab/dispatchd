// Package fakeprovider is a stand-in for an email or push provider with
// switchable faults: temporary 5xx, 429 rate limiting, permanent 4xx and
// slow responses, each applied to a fraction of requests. It records
// every request it receives (time, notification id, attempt, result) so
// tests can count duplicates and see the shape of a retry storm.
package fakeprovider

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Mode is the fault configuration.
type Mode struct {
	Mode  string  `json:"mode"`  // ok | fail_5xx | fail_429 | permanent | slow
	Ratio float64 `json:"ratio"` // fraction of requests the mode applies to; 0 means 1
	// SlowMS is the extra latency for mode=slow.
	SlowMS int `json:"slow_ms,omitempty"`
	// RateLimitRPS > 0 makes the provider itself enforce a rate: requests
	// over it get 429 with Retry-After, like a real provider's quota.
	RateLimitRPS   float64 `json:"rate_limit_rps,omitempty"`
	RateLimitBurst int     `json:"rate_limit_burst,omitempty"`
	// FailFirst makes the first N attempts for a recipient fail with 503,
	// whatever the mode; the learn page uses it for a deterministic retry.
	FailFirst map[string]int `json:"fail_first,omitempty"`
}

// Record is one request the provider received.
type Record struct {
	MS             int64  `json:"ms"` // since the provider's clock origin (reset)
	At             string `json:"at"`
	NotificationID string `json:"notification_id"`
	Attempt        int    `json:"attempt"`
	Recipient      string `json:"recipient"`
	Status         int    `json:"status"`
	Code           string `json:"code,omitempty"`
	Duplicate      bool   `json:"duplicate,omitempty"`
}

// Server is one fake provider.
type Server struct {
	Kind string // "email" or "push"

	mu        sync.Mutex
	mode      Mode
	origin    time.Time
	records   []Record
	delivered map[string]string // idempotency key → message id
	seen      map[string]int    // recipient → attempts seen (for FailFirst)
	tokens    float64
	last      time.Time
	seq       int
}

// New makes a provider of the given kind in mode ok.
func New(kind string) *Server {
	s := &Server{Kind: kind}
	s.reset()
	return s
}

func (s *Server) reset() {
	s.mode = Mode{Mode: "ok"}
	s.origin = time.Now()
	s.records = nil
	s.delivered = map[string]string{}
	s.seen = map[string]int{}
	s.tokens, s.last = 0, time.Time{}
}

// SetMode replaces the fault configuration (keeps the records).
func (s *Server) SetMode(m Mode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.Mode == "" {
		m.Mode = "ok"
	}
	s.mode = m
	s.tokens = float64(max(m.RateLimitBurst, 1))
	s.last = time.Now()
	s.seen = map[string]int{}
}

// Records returns a copy of what the provider received.
func (s *Server) Records() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Record(nil), s.records...)
}

type sendReq struct {
	NotificationID string `json:"notification_id"`
	To             string `json:"to"`
	DeviceToken    string `json:"device_token"`
	Subject        string `json:"subject"`
	Title          string `json:"title"`
	Body           string `json:"body"`
}

// allow is the provider's own token bucket (only when RateLimitRPS > 0).
func (s *Server) allow(now time.Time) (bool, time.Duration) {
	m := s.mode
	if m.RateLimitRPS <= 0 {
		return true, 0
	}
	burst := float64(max(m.RateLimitBurst, 1))
	if s.last.IsZero() {
		s.tokens, s.last = burst, now
	}
	s.tokens = min(burst, s.tokens+now.Sub(s.last).Seconds()*m.RateLimitRPS)
	s.last = now
	if s.tokens >= 1 {
		s.tokens--
		return true, 0
	}
	return false, time.Duration((1 - s.tokens) / m.RateLimitRPS * float64(time.Second))
}

func (s *Server) permanent(recipient string) (int, string, string) {
	if s.Kind == "push" {
		if strings.HasPrefix(recipient, "expired-") {
			return 410, "device_token_unregistered", "the device token is no longer registered: " + recipient
		}
		return 0, "", ""
	}
	if !strings.Contains(recipient, "@") || strings.HasSuffix(recipient, "@invalid.example") {
		return 400, "invalid_recipient", "mailbox does not exist: " + recipient
	}
	return 0, "", ""
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// @learn:fake-provider
func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	var req sendReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "bad_request", "message": err.Error()})
		return
	}
	recipient := req.To
	if s.Kind == "push" {
		recipient = req.DeviceToken
	}
	attempt, _ := strconv.Atoi(r.Header.Get("X-Attempt"))
	key := r.Header.Get("Idempotency-Key")
	now := time.Now()

	s.mu.Lock()
	m := s.mode
	rec := Record{MS: now.Sub(s.origin).Milliseconds(), At: now.UTC().Format(time.RFC3339Nano), NotificationID: req.NotificationID, Attempt: attempt, Recipient: recipient}
	status, code, msg, retryAfter := 200, "", "", time.Duration(0)
	s.seen[recipient]++
	applies := m.Ratio <= 0 || rand.Float64() < m.Ratio
	if ok, wait := s.allow(now); !ok {
		status, code, msg, retryAfter = 429, "rate_limited", "provider quota exceeded", wait
	} else if n := m.FailFirst[recipient]; n > 0 && s.seen[recipient] <= n {
		status, code, msg = 503, "unavailable", "temporarily unavailable (injected for this recipient)"
	} else if applies && m.Mode == "fail_5xx" {
		status, code, msg = 503, "unavailable", "temporarily unavailable (injected)"
	} else if applies && m.Mode == "fail_429" {
		status, code, msg, retryAfter = 429, "rate_limited", "provider quota exceeded (injected)", time.Second
	} else if applies && m.Mode == "permanent" {
		status, code, msg = 400, "rejected", "provider rejected the message (injected permanent failure)"
	} else if st, c, mm := s.permanent(recipient); st != 0 {
		status, code, msg = st, c, mm
	}
	slow := applies && m.Mode == "slow" && status == 200
	var msgID string
	if status == 200 {
		if prev, dup := s.delivered[key]; dup && key != "" {
			msgID, rec.Duplicate = prev, true
		} else {
			s.seq++
			msgID = fmt.Sprintf("%s-msg-%d", s.Kind, s.seq)
			if key != "" {
				s.delivered[key] = msgID
			}
		}
	}
	rec.Status, rec.Code = status, code
	if len(s.records) < 500_000 {
		s.records = append(s.records, rec)
	}
	s.mu.Unlock()

	if slow {
		time.Sleep(time.Duration(m.SlowMS) * time.Millisecond)
	}
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.FormatFloat(retryAfter.Seconds(), 'f', 3, 64))
	}
	if status != 200 {
		writeJSON(w, status, map[string]any{"code": code, "message": msg})
		return
	}
	writeJSON(w, 200, map[string]any{"message_id": msgID, "duplicate": rec.Duplicate})
}

// @learn:end fake-provider

// Stats buckets the received requests by time.
type Stats struct {
	Total    int         `json:"total"`
	ByStatus map[int]int `json:"by_status"`
	BucketMS int64       `json:"bucket_ms"`
	Buckets  []Bucket    `json:"buckets"`
	Peak     int         `json:"peak"`
	// Distinct notifications that got a non-duplicate 200.
	Delivered  int `json:"delivered"`
	Duplicates int `json:"duplicates"`
}

// Bucket is the request count in one time window.
type Bucket struct {
	TMS   int64 `json:"t_ms"`
	Count int   `json:"count"`
}

// ComputeStats summarises records into fixed windows.
func ComputeStats(recs []Record, bucketMS int64) Stats {
	if bucketMS <= 0 {
		bucketMS = 100
	}
	st := Stats{ByStatus: map[int]int{}, BucketMS: bucketMS, Buckets: []Bucket{}}
	counts := map[int64]int{}
	var lo, hi int64 = -1, -1
	for _, r := range recs {
		st.Total++
		st.ByStatus[r.Status]++
		if r.Status == 200 {
			if r.Duplicate {
				st.Duplicates++
			} else {
				st.Delivered++
			}
		}
		b := r.MS / bucketMS
		counts[b]++
		if lo < 0 || b < lo {
			lo = b
		}
		if b > hi {
			hi = b
		}
	}
	if lo >= 0 {
		for b := lo; b <= hi; b++ {
			st.Buckets = append(st.Buckets, Bucket{TMS: b * bucketMS, Count: counts[b]})
			st.Peak = max(st.Peak, counts[b])
		}
	}
	return st
}

// Handler serves the provider API and its admin endpoints.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	path := "/v1/send"
	if s.Kind == "push" {
		path = "/v1/push"
	}
	mux.HandleFunc("POST "+path, s.send)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok", "kind": s.Kind})
	})
	mux.HandleFunc("GET /admin/mode", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, 200, s.mode)
	})
	mux.HandleFunc("POST /admin/mode", func(w http.ResponseWriter, r *http.Request) {
		var m Mode
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		switch m.Mode {
		case "", "ok", "fail_5xx", "fail_429", "permanent", "slow":
		default:
			writeJSON(w, 400, map[string]string{"error": "unknown mode " + m.Mode})
			return
		}
		s.SetMode(m)
		writeJSON(w, 200, m)
	})
	mux.HandleFunc("POST /admin/reset", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		s.reset()
		s.mu.Unlock()
		writeJSON(w, 200, map[string]string{"status": "reset"})
	})
	mux.HandleFunc("GET /admin/requests", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("notification_id")
		out := []Record{}
		for _, rec := range s.Records() {
			if id == "" || rec.NotificationID == id {
				out = append(out, rec)
			}
		}
		if n, _ := strconv.Atoi(r.URL.Query().Get("limit")); n > 0 && len(out) > n {
			out = out[len(out)-n:]
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("GET /admin/stats", func(w http.ResponseWriter, r *http.Request) {
		b, _ := strconv.ParseInt(r.URL.Query().Get("bucket_ms"), 10, 64)
		recs := s.Records()
		if since, err := strconv.ParseInt(r.URL.Query().Get("since_ms"), 10, 64); err == nil {
			sort.Slice(recs, func(i, j int) bool { return recs[i].MS < recs[j].MS })
			i := sort.Search(len(recs), func(i int) bool { return recs[i].MS >= since })
			recs = recs[i:]
		}
		st := ComputeStats(recs, b)
		s.mu.Lock()
		now := time.Since(s.origin).Milliseconds()
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"now_ms": now, "stats": st})
	})
	return mux
}
