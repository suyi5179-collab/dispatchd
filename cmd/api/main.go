// api serves the notification API.
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
	"github.com/suyi5179-collab/dispatchd/internal/provider"
	"github.com/suyi5179-collab/dispatchd/internal/store"
	"github.com/suyi5179-collab/dispatchd/internal/worker"
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

	// Chapter 01: deliver in-process. A buffered channel and a few
	// goroutines; a crash loses whatever is in the channel.
	d := &worker.Deliverer{Store: st, Log: log, Providers: map[string]*provider.Client{
		"email": provider.NewClient("email", config.Env("DISPATCHD_EMAIL_URL", "http://localhost:8851"), 5*time.Second),
		"push":  provider.NewClient("push", config.Env("DISPATCHD_PUSH_URL", "http://localhost:8852"), 5*time.Second),
	}}
	queue := make(chan string, 1024)
	for i := 0; i < 4; i++ {
		go func() {
			for id := range queue {
				if _, err := d.Deliver(ctx, id); err != nil {
					log.Error("deliver", "id", id, "err", err)
				}
			}
		}()
	}
	srv := &api.Server{Store: st, Log: log, Enqueue: func(_ context.Context, id string) { queue <- id }}
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
