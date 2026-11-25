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

	"github.com/suyi5179-collab/dispatchd/internal/backoff"
	"github.com/suyi5179-collab/dispatchd/internal/config"
	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/queue"
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
		Providers: map[string]*provider.Client{
			"email": provider.NewClient("email", config.Env("DISPATCHD_EMAIL_URL", "http://localhost:8851"), timeout),
			"push":  provider.NewClient("push", config.Env("DISPATCHD_PUSH_URL", "http://localhost:8852"), timeout),
		}}).Init()
	reload := make(chan struct{}, 1)
	kc, err := worker.ConsumerClient(brokers, config.Env("DISPATCHD_GROUP", "dispatchd-workers"), topics.All(), config.Duration("DISPATCHD_SESSION_TIMEOUT", 10*time.Second),
		func() {
			select {
			case reload <- struct{}{}:
			default:
			}
		})
	if err != nil {
		log.Error("kafka", "err", err)
		os.Exit(1)
	}
	defer kc.Close()

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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	hs := &http.Server{Addr: config.Env("DISPATCHD_WORKER_ADDR", ":8853"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = hs.ListenAndServe() }()
	defer hs.Close()

	c := &worker.Consumer{Client: kc, D: d, Log: log, Concurrency: config.Int("DISPATCHD_CONCURRENCY", 32), Name: name}
	log.Info("worker consuming", "topics", topics.All())
	if err := c.Run(ctx); err != nil {
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
