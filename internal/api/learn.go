package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// The learn page submits one real notification and then reads back, from
// MySQL and the fake provider, what each stage of the pipeline did to it.
// Nothing here is animated: a stage is "done" when its row or event
// exists, and the data shown is that row or event.

// LearnStages are the stages in order; Part ties each to its video.
var LearnStages = []struct{ ID, Part, Title string }{
	{"submit", "p1-intake", "提交样例（带幂等键）"},
	{"idempotency", "p1-intake", "幂等检查：idempotency_keys 那一行"},
	{"persist", "p2-outbox", "同一个事务写 notification + outbox"},
	{"relay", "p2-outbox", "relay 发到 Kafka"},
	{"token", "p3-token", "worker 取到消息，拿令牌"},
	{"attempt1", "p4-retry", "第一次投递：provider 临时故障"},
	{"delay", "p4-retry", "进延迟堆：退避 + jitter"},
	{"retry", "p4-retry", "到期唤醒，重试成功"},
	{"delivered", "p5-delivered", "状态 DELIVERED"},
	{"replay", "p5-delivered", "同一个幂等键再提交一次"},
}

// Stage is what the page shows for one stage.
type Stage struct {
	ID     string         `json:"id"`
	Part   string         `json:"part"`
	Title  string         `json:"title"`
	Status string         `json:"status"` // pending | running | done
	At     *time.Time     `json:"at,omitempty"`
	TMS    *float64       `json:"t_ms,omitempty"` // since the notification was accepted
	Data   map[string]any `json:"data,omitempty"`
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Server) learnRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/learn/sample", s.auth(s.learnSample))
	mux.HandleFunc("POST /v1/learn/runs/{id}/replay", s.auth(s.learnReplay))
	mux.HandleFunc("GET /v1/learn/runs/{id}", s.auth(s.learnRun))
}

// submitInternally sends a request through the real create handler, so
// the learn page exercises exactly the code a caller does.
func (s *Server) submitInternally(ctx context.Context, req Request, key string) (int, http.Header, json.RawMessage) {
	b, _ := json.Marshal(req)
	r := httptest.NewRequestWithContext(ctx, "POST", "/v1/notifications", bytes.NewReader(b))
	r.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	s.create(w, r)
	return w.Code, w.Header(), w.Body.Bytes()
}

// @learn:learn-sample
func (s *Server) learnSample(w http.ResponseWriter, r *http.Request) {
	recipient := "learn-" + randHex(3) + "@example.com"
	// Make the email provider fail the first attempt for this recipient
	// only, so the sample shows a real retry without disturbing other
	// traffic.
	var mode fakeprovider.Mode
	if err := s.providerJSON(r.Context(), "email", "GET", "/admin/mode", nil, &mode); err != nil {
		Error(w, 502, "email provider: "+err.Error())
		return
	}
	if mode.FailFirst == nil {
		mode.FailFirst = map[string]int{}
	}
	mode.FailFirst[recipient] = 1
	if err := s.providerJSON(r.Context(), "email", "POST", "/admin/mode", mode, nil); err != nil {
		Error(w, 502, "email provider: "+err.Error())
		return
	}
	req := Request{Channel: "email", Recipient: recipient, Template: "order_shipped", Params: map[string]any{"order_id": "A-1042"}}
	key := "learn-" + randHex(6)
	code, hdr, body := s.submitInternally(r.Context(), req, key)
	JSON(w, 200, map[string]any{"request": req, "idempotency_key": key, "status_code": code, "replayed": hdr.Get("Idempotent-Replayed") == "true", "response": body})
}

// @learn:end learn-sample

func (s *Server) learnReplay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.Store.Notification(r.Context(), ClientFrom(r.Context()).ID, id); err != nil {
		Error(w, 404, "no such notification")
		return
	}
	idem, err := s.Store.IdemForNotification(r.Context(), id)
	if err != nil {
		Error(w, 404, "no idempotency key for this notification")
		return
	}
	events, _ := s.Store.Events(r.Context(), id)
	var req Request
	for _, e := range events {
		if e.Stage == "accepted" {
			var d struct {
				Request Request `json:"request"`
			}
			_ = json.Unmarshal(e.Data, &d)
			req = d.Request
		}
	}
	code, hdr, body := s.submitInternally(r.Context(), req, idem.Key)
	JSON(w, 200, map[string]any{"request": req, "idempotency_key": idem.Key, "status_code": code, "replayed": hdr.Get("Idempotent-Replayed") == "true", "response": body})
}

func asMap(raw json.RawMessage) map[string]any {
	m := map[string]any{}
	_ = json.Unmarshal(raw, &m)
	return m
}

// @learn:learn-stages
func (s *Server) learnRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	n, err := s.Store.Notification(ctx, ClientFrom(ctx).ID, id)
	if err != nil {
		Error(w, 404, "no such notification")
		return
	}
	events, _ := s.Store.Events(ctx, id)
	outbox, _ := s.Store.OutboxFor(ctx, id)
	attempts, _ := s.Store.Attempts(ctx, id)
	idem, _ := s.Store.IdemForNotification(ctx, id)
	var provider []fakeprovider.Record
	_ = s.providerJSON(ctx, n.Channel, "GET", "/admin/requests?notification_id="+id, nil, &provider)

	byStage := map[string][]store.Event{}
	for _, e := range events {
		byStage[e.Stage] = append(byStage[e.Stage], e)
	}
	first := func(stage string) *store.Event {
		if l := byStage[stage]; len(l) > 0 {
			return &l[0]
		}
		return nil
	}
	nth := func(stage string, i int) *store.Event {
		if l := byStage[stage]; len(l) > i {
			return &l[i]
		}
		return nil
	}
	stages := make([]*Stage, len(LearnStages))
	for i, st := range LearnStages {
		stages[i] = &Stage{ID: st.ID, Part: st.Part, Title: st.Title, Status: "pending"}
	}
	t0 := n.CreatedAt
	done := func(i int, at time.Time, data map[string]any) {
		ms := float64(at.Sub(t0)) / float64(time.Millisecond)
		stages[i].Status, stages[i].At, stages[i].TMS, stages[i].Data = "done", &at, &ms, data
	}

	if e := first("accepted"); e != nil {
		d := asMap(e.Data)
		data := map[string]any{"request": d["request"], "idempotency_key": d["idempotency_key"]}
		if idem != nil {
			data["status_code"], data["response"] = idem.StatusCode, idem.Response
		}
		done(0, n.CreatedAt, data) // the moment the API took the request
	}
	if idem != nil {
		done(1, idem.CreatedAt, map[string]any{"row": idem, "request_hash": idem.RequestHash})
	}
	if len(outbox) > 0 {
		done(2, outbox[0].CreatedAt, map[string]any{"outbox_row": outbox[0], "notification_row": map[string]any{
			"id": n.ID, "channel": n.Channel, "recipient": n.Recipient, "template": n.Template, "status": store.StatusQueued, "created_at": n.CreatedAt}})
	}
	// The relay commits after Kafka acknowledged, so a fast worker can
	// consume the message before "published" is written. Either event
	// proves the message is in Kafka.
	if pub, con := first("published"), first("consumed"); pub != nil || con != nil {
		data := map[string]any{}
		at := time.Time{}
		if pub != nil {
			d := asMap(pub.Data)
			data["topic"], data["partition"], data["offset"], data["key"], data["message"] = d["topic"], d["partition"], d["offset"], d["key"], d["payload"]
			at = pub.At
		}
		if con != nil {
			data["consumed"] = asMap(con.Data)
			if at.IsZero() || con.At.Before(at) {
				at = con.At
			}
			c := asMap(con.Data)
			for _, k := range []string{"topic", "partition", "offset"} {
				if data[k] == nil {
					data[k] = c[k]
				}
			}
		}
		done(3, at, data)
	}
	if e := first("claimed"); e != nil {
		done(4, e.At, map[string]any{"claim": asMap(e.Data), "transition": map[string]string{"from": e.From, "to": e.To}})
	}
	if e := first("attempt"); e != nil {
		d := asMap(e.Data)
		data := map[string]any{"attempt": d["attempt"], "http_status": d["http_status"], "outcome": d["outcome"], "provider_response": d["provider_response"], "transition": map[string]string{"from": e.From, "to": e.To}}
		if len(attempts) > 0 {
			data["attempt_row"] = attempts[0]
		}
		done(5, e.At, data)
		if b, ok := d["backoff"]; ok {
			data := map[string]any{"backoff": b, "next_attempt_at": d["next_attempt_at"]}
			at := e.At
			if dl := first("delayed"); dl != nil {
				data["delay_queue"] = asMap(dl.Data)
				at = dl.At
			}
			done(6, at, data)
		}
	}
	if wk, a2 := first("woken"), nth("attempt", 1); wk != nil && a2 != nil {
		d := asMap(a2.Data)
		data := map[string]any{"woken": asMap(wk.Data), "attempt": d["attempt"], "http_status": d["http_status"], "outcome": d["outcome"], "provider_response": d["provider_response"]}
		if c2 := nth("claimed", 1); c2 != nil {
			data["claim"] = asMap(c2.Data)
		}
		done(7, wk.At, data) // the moment the delay queue woke up
	}
	if n.Status == store.StatusDelivered && n.DeliveredAt != nil {
		var trans []map[string]any
		for _, e := range events {
			if e.To != "" {
				trans = append(trans, map[string]any{"stage": e.Stage, "from": e.From, "to": e.To, "at": e.At})
			}
		}
		done(8, *n.DeliveredAt, map[string]any{"transitions": trans, "status": n.Status, "attempts": n.Attempts, "delivered_at": n.DeliveredAt})
	}
	if e := first("replayed"); e != nil {
		ok200 := 0
		for _, p := range provider {
			if p.Status == 200 && !p.Duplicate {
				ok200++
			}
		}
		var sameKey int
		if idem != nil {
			_ = s.Store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM idempotency_keys WHERE client_id=? AND idem_key=?`, idem.ClientID, idem.Key).Scan(&sameKey)
		}
		done(9, e.At, map[string]any{"replays": len(byStage["replayed"]), "notification_id": n.ID, "rows_with_this_key": sameKey,
			"provider_requests": provider, "provider_deliveries": ok200})
	}
	// The first stage not done after a done one is in progress.
	for i, st := range stages {
		if st.Status == "pending" && (i == 0 || stages[i-1].Status == "done") && i < 9 {
			st.Status = "running"
			break
		}
	}
	JSON(w, 200, map[string]any{"notification": n, "stages": stages, "provider_requests": provider})
}

// @learn:end learn-stages
