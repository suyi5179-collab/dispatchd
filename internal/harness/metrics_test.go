package harness_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/harness"
	"github.com/suyi5179-collab/dispatchd/internal/metrics"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// The runbook reads these series; a rename would silently break it and
// the alert rules. Run real traffic, then scrape.
func TestMetricsTheRunbookReadsAreExported(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	s.Email.SetMode(fakeprovider.Mode{Mode: "ok", FailFirst: map[string]int{"user1@example.com": 1}})
	id := s.Submit(email(1))
	s.WaitStatus(id, store.StatusDelivered, 15*time.Second)
	s.Do("POST", "/v1/notifications", email(2), "same")
	s.Do("POST", "/v1/notifications", email(2), "same")
	w := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(w.Body)
	for _, want := range []string{
		`dispatchd_attempts_total{outcome="DELIVERED",provider="email"}`,
		`dispatchd_attempts_total{outcome="TEMPORARY",provider="email"}`,
		`dispatchd_provider_errors_total{code="unavailable",kind="TEMPORARY",provider="email"}`,
		`dispatchd_delivery_latency_seconds_bucket{channel="email"`,
		`dispatchd_retries_scheduled_total{provider="email"}`,
		`dispatchd_notifications_final_total{channel="email",status="DELIVERED"}`,
		`dispatchd_idempotent_replays_total`,
		`dispatchd_outbox_unpublished`,
		`dispatchd_provider_request_seconds_bucket{provider="email"`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing %s", want)
		}
	}
}
