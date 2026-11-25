// Package metrics defines the Prometheus metrics. The three questions the
// runbook starts from: is the queue backing up (Kafka lag, outbox, delay
// queue), are deliveries slow (latency histograms), and is a provider
// failing (attempts and errors by provider, kind and code).
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler serves /metrics.
func Handler() http.Handler { return promhttp.Handler() }

var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// API side.
var (
	Accepted = promauto.NewCounterVec(prometheus.CounterOpts{Name: "dispatchd_notifications_accepted_total",
		Help: "New notifications stored by the API."}, []string{"channel"})
	Replays = promauto.NewCounter(prometheus.CounterOpts{Name: "dispatchd_idempotent_replays_total",
		Help: "Requests answered from a stored Idempotency-Key instead of creating a notification."})
	KeyConflicts = promauto.NewCounter(prometheus.CounterOpts{Name: "dispatchd_idempotency_conflicts_total",
		Help: "Idempotency-Key reused with a different body (422)."})
	OutboxUnpublished = promauto.NewGauge(prometheus.GaugeOpts{Name: "dispatchd_outbox_unpublished",
		Help: "Outbox rows not yet in Kafka."})
	Published = promauto.NewCounter(prometheus.CounterOpts{Name: "dispatchd_relay_published_total",
		Help: "Outbox rows published to Kafka."})
)

// Worker side.
var (
	Attempts = promauto.NewCounterVec(prometheus.CounterOpts{Name: "dispatchd_attempts_total",
		Help: "Delivery attempts by provider and outcome (DELIVERED, TEMPORARY, RATE_LIMITED, PERMANENT)."}, []string{"provider", "outcome"})
	ProviderErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "dispatchd_provider_errors_total",
		Help: "Failed attempts by provider, kind and provider error code."}, []string{"provider", "kind", "code"})
	ProviderSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "dispatchd_provider_request_seconds",
		Help: "Provider HTTP call latency.", Buckets: latencyBuckets}, []string{"provider"})
	DeliveryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "dispatchd_delivery_latency_seconds",
		Help: "From accepted by the API to delivered by the provider, including retries.", Buckets: latencyBuckets}, []string{"channel"})
	Final = promauto.NewCounterVec(prometheus.CounterOpts{Name: "dispatchd_notifications_final_total",
		Help: "Notifications reaching a final status."}, []string{"channel", "status"})
	RetriesScheduled = promauto.NewCounterVec(prometheus.CounterOpts{Name: "dispatchd_retries_scheduled_total",
		Help: "Retries put into the delay queue."}, []string{"provider"})
	TokenWait = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "dispatchd_ratelimit_wait_seconds",
		Help: "Time spent waiting for a token-bucket token before an attempt.", Buckets: []float64{0, .001, .01, .05, .1, .25, .5, 1, 2, 5, 10}}, []string{"provider"})
	KafkaLag = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "dispatchd_kafka_consumer_lag",
		Help: "Messages in Kafka not yet committed by the worker group (the queue depth)."}, []string{"topic"})
	LeaseExpired = promauto.NewCounter(prometheus.CounterOpts{Name: "dispatchd_lease_expired_total",
		Help: "Attempts abandoned by a dead worker and retried after their lease ran out."})
)

// GaugeFunc registers a gauge computed on scrape.
func GaugeFunc(name, help string, labels prometheus.Labels, f func() float64) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels}, f)
}

// CounterFunc registers a counter computed on scrape.
func CounterFunc(name, help string, labels prometheus.Labels, f func() float64) {
	promauto.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: labels}, f)
}

// Since returns seconds since t.
func Since(t time.Time) float64 { return time.Since(t).Seconds() }
