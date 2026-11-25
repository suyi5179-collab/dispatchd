// fakeprovider runs a fake email or push provider with fault injection.
//
//	fakeprovider -kind email -addr :8851
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"

	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
)

func main() {
	kind := flag.String("kind", "email", "email or push")
	addr := flag.String("addr", ":8851", "listen address")
	flag.Parse()
	s := fakeprovider.New(*kind)
	slog.Info("fake provider listening", "kind", *kind, "addr", *addr)
	if err := http.ListenAndServe(*addr, s.Handler()); err != nil {
		slog.Error("fake provider stopped", "err", err)
		os.Exit(1)
	}
}
