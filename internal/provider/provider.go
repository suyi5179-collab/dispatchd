// Package provider sends a notification to an email or push provider and
// classifies the answer: delivered, temporary (retry later), rate limited
// (retry after the provider's hint), or permanent (never retry; record
// what a person should do about it).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Kind of result.
type Kind string

const (
	Delivered   Kind = "DELIVERED"
	Temporary   Kind = "TEMPORARY"
	RateLimited Kind = "RATE_LIMITED"
	Permanent   Kind = "PERMANENT"
)

// Result of one send.
type Result struct {
	Kind       Kind          `json:"kind"`
	HTTPStatus int           `json:"http_status"`
	Code       string        `json:"code,omitempty"`
	Message    string        `json:"message,omitempty"`
	Body       string        `json:"body,omitempty"` // the provider's raw answer (truncated)
	MessageID  string        `json:"message_id,omitempty"`
	Duplicate  bool          `json:"duplicate,omitempty"`
	RetryAfter time.Duration `json:"-"`
	Latency    time.Duration `json:"-"`
	Action     string        `json:"action,omitempty"` // what to do about a permanent failure
}

// Templates the service knows. Params fill {{name}} placeholders.
var Templates = map[string][2]string{
	"welcome":       {"Welcome, {{name}}", "Hi {{name}}, thanks for signing up."},
	"otp":           {"Your sign-in code", "Your code is {{code}}. It expires in 10 minutes."},
	"order_shipped": {"Order {{order_id}} shipped", "Your order {{order_id}} is on its way."},
}

// Render fills a template.
func Render(template string, params map[string]any) (string, string, error) {
	t, ok := Templates[template]
	if !ok {
		return "", "", fmt.Errorf("unknown template %q", template)
	}
	fill := func(s string) string {
		for k, v := range params {
			s = strings.ReplaceAll(s, "{{"+k+"}}", fmt.Sprint(v))
		}
		return s
	}
	return fill(t[0]), fill(t[1]), nil
}

// Message is what gets sent.
type Message struct {
	NotificationID string
	Attempt        int
	Recipient      string
	Template       string
	Params         map[string]any
}

// Client talks to one provider.
type Client struct {
	Name string // "email" or "push"
	URL  string // base URL
	HTTP *http.Client
}

// NewClient with a per-request timeout.
func NewClient(name, url string, timeout time.Duration) *Client {
	return &Client{Name: name, URL: strings.TrimRight(url, "/"), HTTP: &http.Client{Timeout: timeout}}
}

// Send delivers one message. The Idempotency-Key is the notification id,
// so a provider that supports keys collapses a resend of the same
// notification (the crash window between "provider said 200" and "we
// wrote DELIVERED").
func (c *Client) Send(ctx context.Context, m Message) Result {
	subject, body, err := Render(m.Template, m.Params)
	if err != nil {
		return Result{Kind: Permanent, Code: "unknown_template", Message: err.Error(), Action: "fix the template name in the request"}
	}
	payload := map[string]any{"notification_id": m.NotificationID, "body": body}
	path := "/v1/send"
	if c.Name == "push" {
		path = "/v1/push"
		payload["device_token"] = m.Recipient
		payload["title"] = subject
	} else {
		payload["to"] = m.Recipient
		payload["subject"] = subject
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", c.URL+path, bytes.NewReader(b))
	if err != nil {
		return Result{Kind: Permanent, Code: "bad_request", Message: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.NotificationID)
	req.Header.Set("X-Attempt", strconv.Itoa(m.Attempt))
	start := time.Now()
	resp, err := c.HTTP.Do(req)
	lat := time.Since(start)
	if err != nil {
		code := "network_error"
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			code = "timeout"
		}
		return Result{Kind: Temporary, Code: code, Message: err.Error(), Latency: lat}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return Classify(resp.StatusCode, resp.Header.Get("Retry-After"), raw, lat)
}

// Classify maps a provider answer to a Result.
// @learn:classify
func Classify(status int, retryAfter string, raw []byte, lat time.Duration) Result {
	var body struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		MessageID string `json:"message_id"`
		Duplicate bool   `json:"duplicate"`
	}
	_ = json.Unmarshal(raw, &body)
	r := Result{HTTPStatus: status, Code: body.Code, Message: body.Message, Body: string(raw), MessageID: body.MessageID, Duplicate: body.Duplicate, Latency: lat}
	switch {
	case status >= 200 && status < 300:
		r.Kind = Delivered
	case status == 429:
		r.Kind = RateLimited
		if s, err := strconv.ParseFloat(retryAfter, 64); err == nil && s > 0 {
			r.RetryAfter = time.Duration(s * float64(time.Second))
		}
	case status == 408 || status >= 500:
		r.Kind = Temporary
	default: // other 4xx: retrying the same request cannot succeed
		r.Kind = Permanent
		r.Action = Action(status, body.Code)
	}
	return r
}

// @learn:end classify

// Action is the advice stored with a permanent failure, so the person who
// reads the record knows what to fix.
func Action(status int, code string) string {
	switch code {
	case "invalid_recipient":
		return "the address does not exist: correct it with the user; do not retry"
	case "device_token_unregistered":
		return "remove this device token from the user's profile; the app was uninstalled or the token rotated"
	case "rejected":
		return "the provider refused this message: check its content against the provider's policy"
	}
	switch status {
	case 401, 403:
		return "the provider rejected our credentials: rotate the provider API key"
	case 413:
		return "the message is too large for this channel: shorten the template"
	}
	return fmt.Sprintf("provider returned %d %s: inspect the raw response", status, code)
}
