// loadgen drives the running compose stack with an open-model load (requests
// leave on a fixed schedule, not when the previous one returns), waits until
// every notification is final, and reports API latency, end-to-end delivery
// latency (accepted → provider 200, read from MySQL) and throughput.
//
//	go run ./cmd/loadgen -rate 60 -duration 20s -out docs/results/loadtest-60rps.json
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	i := int(p*float64(len(xs))+0.5) - 1
	return xs[max(0, min(i, len(xs)-1))]
}

func main() {
	api := flag.String("api", "http://localhost:8850", "API base URL")
	key := flag.String("key", "dk_demo_local_only", "API key")
	dsn := flag.String("dsn", "dispatchd:dispatchd@tcp(localhost:33518)/dispatchd?parseTime=true", "MySQL DSN (to read delivered_at)")
	rate := flag.Float64("rate", 60, "notifications per second (half email, half push)")
	dur := flag.Duration("duration", 20*time.Second, "how long to send")
	out := flag.String("out", "", "write the summary JSON here")
	flag.Parse()

	client := &http.Client{Timeout: 10 * time.Second}
	n := int(*rate * dur.Seconds())
	interval := time.Duration(float64(time.Second) / *rate)
	ids := make([]string, n)
	lat := make([]float64, n)
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := time.Now()
	run := fmt.Sprintf("lg%d", start.Unix())
	for i := range n {
		at := start.Add(time.Duration(i) * interval)
		time.Sleep(time.Until(at))
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := map[string]any{"channel": "email", "recipient": fmt.Sprintf("%s-%d@example.com", run, i), "template": "welcome", "params": map[string]any{"name": i}}
			if i%2 == 1 {
				body = map[string]any{"channel": "push", "recipient": fmt.Sprintf("%s-device-%d", run, i), "template": "otp", "params": map[string]any{"code": rand.IntN(1_000_000)}}
			}
			b, _ := json.Marshal(body)
			req, _ := http.NewRequest("POST", *api+"/v1/notifications", bytes.NewReader(b))
			req.Header.Set("Authorization", "Bearer "+*key)
			req.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%d", run, i))
			resp, err := client.Do(req)
			lat[i] = float64(time.Since(at)) / float64(time.Millisecond) // from the scheduled send time
			if err != nil {
				return
			}
			defer resp.Body.Close()
			codes[i] = resp.StatusCode
			var r struct{ ID string }
			_ = json.NewDecoder(resp.Body).Decode(&r)
			ids[i] = r.ID
		}()
	}
	wg.Wait()
	sent := time.Since(start)

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	var valid []string
	for _, id := range ids {
		if id != "" {
			valid = append(valid, id)
		}
	}
	in := "'" + strings.Join(valid, "','") + "'"
	ctx := context.Background()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var pending int
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notifications WHERE id IN (`+in+`) AND status IN ('QUEUED','SENDING','RETRY_SCHEDULED')`).Scan(&pending)
		if pending == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	rows, err := db.QueryContext(ctx, `SELECT channel, status, TIMESTAMPDIFF(MICROSECOND, created_at, delivered_at), delivered_at FROM notifications WHERE id IN (`+in+`)`)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	e2e := map[string][]float64{}
	status := map[string]int{}
	var last time.Time
	for rows.Next() {
		var ch, st string
		var us sql.NullInt64
		var at sql.NullTime
		_ = rows.Scan(&ch, &st, &us, &at)
		status[st]++
		if us.Valid {
			e2e[ch] = append(e2e[ch], float64(us.Int64)/1000)
			e2e["all"] = append(e2e["all"], float64(us.Int64)/1000)
		}
		if at.Valid && at.Time.After(last) {
			last = at.Time
		}
	}
	ok := 0
	for _, c := range codes {
		if c == 202 {
			ok++
		}
	}
	summary := map[string]any{
		"rate_per_s": *rate, "duration_s": dur.Seconds(), "sent": n, "accepted_202": ok, "send_window_s": sent.Seconds(),
		"api_latency_ms":  map[string]float64{"p50": pct(append([]float64(nil), lat...), .5), "p99": pct(append([]float64(nil), lat...), .99), "max": pct(append([]float64(nil), lat...), 1)},
		"final_statuses":  status,
		"delivered_per_s": float64(status["DELIVERED"]) / last.Sub(start).Seconds(),
	}
	for _, ch := range []string{"all", "email", "push"} {
		xs := e2e[ch]
		summary["e2e_ms_"+ch] = map[string]float64{"p50": pct(append([]float64(nil), xs...), .5), "p95": pct(append([]float64(nil), xs...), .95), "p99": pct(append([]float64(nil), xs...), .99), "max": pct(append([]float64(nil), xs...), 1)}
	}
	b, _ := json.MarshalIndent(summary, "", " ")
	fmt.Println(string(b))
	if *out != "" {
		_ = os.WriteFile(*out, append(b, '\n'), 0o644)
	}
}
