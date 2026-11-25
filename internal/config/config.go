// Package config reads settings from the environment and holds the
// sabotage switches: named protections that can be turned off at runtime
// so a test can prove it notices when a protection is missing.
package config

import (
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Env returns the variable or a default.
func Env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Duration parses a Go duration from the environment.
func Duration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// Float parses a float from the environment.
func Float(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// Int parses an int from the environment.
func Int(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// Sabotage names. Each one turns a protection off.
const (
	NoIdempotency = "idempotency" // the API ignores Idempotency-Key
	NoJitter      = "jitter"      // retries wait exactly base*2^n
	NoRateLimit   = "ratelimit"   // the per-provider token bucket lets everything through
)

var (
	mu       sync.RWMutex
	sabotage = parse(os.Getenv("DISPATCHD_SABOTAGE"))
)

func parse(s string) map[string]bool {
	m := map[string]bool{}
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			m[p] = true
		}
	}
	return m
}

// Sabotaged reports whether a protection is switched off.
func Sabotaged(name string) bool {
	mu.RLock()
	defer mu.RUnlock()
	return sabotage[name]
}

// SetSabotage switches a protection off (true) or back on (false).
func SetSabotage(name string, off bool) {
	mu.Lock()
	defer mu.Unlock()
	if off {
		sabotage[name] = true
	} else {
		delete(sabotage, name)
	}
}

// Sabotages lists the protections that are switched off.
func Sabotages() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(sabotage))
	for k := range sabotage {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
