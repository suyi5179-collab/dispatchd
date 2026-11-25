package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/config"
)

// The UI switches the fake providers' fault modes and reads what they
// received through these admin routes (admin role only).
func (s *Server) adminRoutes(mux *http.ServeMux) {
	admin := func(h http.HandlerFunc) http.HandlerFunc {
		return s.auth(func(w http.ResponseWriter, r *http.Request) {
			if ClientFrom(r.Context()).Role != "admin" {
				Error(w, 403, "admin API key required")
				return
			}
			h(w, r)
		})
	}
	proxy := func(method, path string) http.HandlerFunc {
		return admin(func(w http.ResponseWriter, r *http.Request) {
			base, ok := s.ProviderURLs[r.PathValue("name")]
			if !ok {
				Error(w, 404, "no such provider")
				return
			}
			body, _ := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
			code, out, err := s.providerCall(r.Context(), method, base+path+"?"+r.URL.RawQuery, body)
			if err != nil {
				Error(w, 502, err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = w.Write(out)
		})
	}
	mux.HandleFunc("GET /v1/admin/providers/{name}/mode", proxy("GET", "/admin/mode"))
	mux.HandleFunc("POST /v1/admin/providers/{name}/mode", proxy("POST", "/admin/mode"))
	mux.HandleFunc("GET /v1/admin/providers/{name}/stats", proxy("GET", "/admin/stats"))
	mux.HandleFunc("GET /v1/admin/providers/{name}/requests", proxy("GET", "/admin/requests"))
	mux.HandleFunc("POST /v1/admin/providers/{name}/reset", proxy("POST", "/admin/reset"))
	mux.HandleFunc("GET /v1/admin/protections", admin(func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, 200, map[string]any{"api_sabotaged": config.Sabotages()})
	}))
}

func (s *Server) providerCall(ctx context.Context, method, url string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return resp.StatusCode, out, err
}

func (s *Server) providerJSON(ctx context.Context, name, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		body, _ = json.Marshal(in)
	}
	_, b, err := s.providerCall(ctx, method, s.ProviderURLs[name]+path, body)
	if err != nil {
		return err
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}
