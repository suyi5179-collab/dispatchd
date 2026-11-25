// worker consumes notification messages from Kafka and delivers them.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/suyi5179-collab/dispatchd/internal/backoff"
	"github.com/suyi5179-collab/dispatchd/internal/config"
	"github.com/suyi5179-collab/dispatchd/internal/metrics"
	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/queue"
	"github.com/suyi5179-collab/dispatchd/internal/ratelimit"
	"github.com/suyi5179-collab/dispatchd/internal/store"
	"github.com/suyi5179-collab/dispatchd/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	name := config.Env("DISPATCHD_WORKER_NAME", hostname())
	log = log.With("worker", name)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, config.Env("DISPATCHD_MYSQL_DSN", "dispatchd:dispatchd@tcp(localhost:33518)/dispatchd"))
	if err != nil {
		log.Error("mysql", "err", err)
		os.Exit(1)
	}
	if err := st.Migrate(ctx); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}
	brokers := queue.Brokers(config.Env("DISPATCHD_KAFKA_BROKERS", "localhost:19193"))
	topics := queue.Topics{Prefix: config.Env("DISPATCHD_TOPIC_PREFIX", "")}
	if err := queue.EnsureTopics(ctx, brokers, 3, topics.All()...); err != nil {
		log.Error("topics", "err", err)
		os.Exit(1)
	}
	timeout := config.Duration("DISPATCHD_PROVIDER_TIMEOUT", 5*time.Second)
	d := (&worker.Deliverer{Store: st, Log: log, Name: name, Lease: config.Duration("DISPATCHD_LEASE", 15*time.Second),
		Concurrency: config.Int("DISPATCHD_CONCURRENCY", 32),
		Backoff: backoff.Policy{
			Base:        config.Duration("DISPATCHD_BACKOFF_BASE", time.Second),
			Cap:         config.Duration("DISPATCHD_BACKOFF_CAP", 2*time.Minute),
			MaxAttempts: config.Int("DISPATCHD_MAX_ATTEMPTS", 8),
		},
		Limits: map[string]*ratelimit.Bucket{
			"email": ratelimit.New(config.Float("DISPATCHD_RATE_EMAIL", 40), config.Float("DISPATCHD_BURST_EMAIL", 10)),
			"push":  ratelimit.New(config.Float("DISPATCHD_RATE_PUSH", 80), config.Float("DISPATCHD_BURST_PUSH", 20)),
		},
		Providers: map[string]*provider.Client{
			"email": provider.NewClient("email", config.Env("DISPATCHD_EMAIL_URL", "http://localhost:8851"), timeout),
			"push":  provider.NewClient("push", config.Env("DISPATCHD_PUSH_URL", "http://localhost:8852"), timeout),
		}}).Init()
	reload := make(chan struct{}, 1)
	group := config.Env("DISPATCHD_GROUP", "dispatchd-workers")
	consumers, err := worker.NewConsumers(brokers, group, topics, config.Duration("DISPATCHD_SESSION_TIMEOUT", 10*time.Second), d, log,
		config.Int("DISPATCHD_CONCURRENCY", 32), name, func() {
			select {
			case reload <- struct{}{}:
			default:
			}
		})
	if err != nil {
		log.Error("kafka", "err", err)
		os.Exit(1)
	}
	defer worker.CloseAll(consumers)

	go d.RunScheduler(ctx)
	go func() {
		for {
			if n, err := d.Reload(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("reload", "err", err)
			} else {
				log.Info("delay queue reloaded from MySQL", "items", n)
			}
			select {
			case <-ctx.Done():
				return
			case <-reload:
			}
		}
	}()

	for _, ch := range worker.Channels {
		go worker.WatchLag(ctx, log, brokers, worker.GroupFor(group, ch), []string{topics.For(ch)}, 5*time.Second)
	}
	metrics.GaugeFunc("dispatchd_delay_queue_size", "Retries and lease checks waiting in this worker's min-heap.", nil, func() float64 { return float64(d.Delay.Len()) })
	metrics.CounterFunc("dispatchd_delay_queue_wakeups_total", "Times the delay queue goroutine woke up.", prometheus.Labels{"reason": "timer"}, func() float64 { return float64(d.Delay.TimerFires.Load()) })
	metrics.CounterFunc("dispatchd_delay_queue_wakeups_total", "Times the delay queue goroutine woke up.", prometheus.Labels{"reason": "earlier_item"}, func() float64 { return float64(d.Delay.Kicks.Load()) })
	metrics.CounterFunc("dispatchd_duplicate_messages_skipped_total", "Kafka messages whose attempt was already claimed or finished.", nil, func() float64 { return float64(worker.Duplicates.Load()) })
	for name, b := range d.Limits {
		metrics.GaugeFunc("dispatchd_ratelimit_tokens", "Tokens left in the provider's bucket (negative: callers waiting).", prometheus.Labels{"provider": name}, b.Tokens)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	hs := &http.Server{Addr: config.Env("DISPATCHD_WORKER_ADDR", ":8853"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = hs.ListenAndServe() }()
	defer hs.Close()

	log.Info("worker consuming", "topics", topics.All())
	if err := worker.RunAll(ctx, consumers); err != nil {
		log.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
	log.Info("worker stopped")
}

func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "worker"
	}
	return h
}
