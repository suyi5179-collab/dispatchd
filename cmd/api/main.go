// api serves the notification API and runs the outbox relay.
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

	"github.com/suyi5179-collab/dispatchd/internal/api"
	"github.com/suyi5179-collab/dispatchd/internal/config"
	"github.com/suyi5179-collab/dispatchd/internal/queue"
	"github.com/suyi5179-collab/dispatchd/internal/relay"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
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
	if err := api.SeedClients(ctx, st, config.Env("DISPATCHD_API_KEYS", api.DefaultAPIKeys)); err != nil {
		log.Error("seed clients", "err", err)
		os.Exit(1)
	}
	brokers := queue.Brokers(config.Env("DISPATCHD_KAFKA_BROKERS", "localhost:19193"))
	topics := queue.Topics{Prefix: config.Env("DISPATCHD_TOPIC_PREFIX", "")}
	if err := queue.EnsureTopics(ctx, brokers, 3, topics.All()...); err != nil {
		log.Error("topics", "err", err)
		os.Exit(1)
	}
	kc, err := queue.Producer(brokers)
	if err != nil {
		log.Error("kafka", "err", err)
		os.Exit(1)
	}
	defer kc.Close()
	rl := relay.New(st, kc, log)
	go rl.Run(ctx)

	srv := &api.Server{Store: st, Log: log, Topics: topics, Enqueue: func(context.Context, string) { rl.Wake() },
		ProviderURLs: map[string]string{
			"email": config.Env("DISPATCHD_EMAIL_URL", "http://localhost:8851"),
			"push":  config.Env("DISPATCHD_PUSH_URL", "http://localhost:8852"),
		}}
	go srv.ExpireKeys(ctx, 24*time.Hour, 10*time.Minute)
	addr := config.Env("DISPATCHD_API_ADDR", ":8850")
	hs := &http.Server{Addr: addr, Handler: srv.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
	}()
	log.Info("api listening", "addr", addr)
	if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http", "err", err)
		os.Exit(1)
	}
}
