// Package testenv gives integration tests their own throwaway MySQL
// database (and, from chapter 03, their own Kafka topics), so tests can
// run in parallel and never see each other's rows.
//
// The databases live in the compose MySQL (scripts/test.sh starts it).
// If it is not reachable the test FAILS rather than skips: a skipped
// integration test looks exactly like a passing one.
// Set DISPATCHD_SKIP_INTEGRATION=1 to skip on purpose.
package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/suyi5179-collab/dispatchd/internal/queue"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// ServerDSN is the MySQL server (no database) the tests use.
func ServerDSN() string {
	if v := os.Getenv("DISPATCHD_TEST_MYSQL_DSN"); v != "" {
		return v
	}
	return "dispatchd:dispatchd@tcp(localhost:33518)/"
}

// Integration skips only when asked to.
func Integration(t testing.TB) {
	t.Helper()
	if os.Getenv("DISPATCHD_SKIP_INTEGRATION") == "1" {
		t.Skip("DISPATCHD_SKIP_INTEGRATION=1")
	}
}

// MySQL creates a fresh, migrated database for one test and drops it
// afterwards.
func MySQL(t testing.TB) *store.Store {
	t.Helper()
	Integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := sql.Open("mysql", ServerDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("MySQL for integration tests is not reachable at %s (run scripts/test.sh, or set DISPATCHD_SKIP_INTEGRATION=1 to skip on purpose): %v", ServerDSN(), err)
	}
	name := fmt.Sprintf("dispatchd_t_%d_%06d", time.Now().Unix()%1_000_000, rand.IntN(1_000_000))
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, _ := mysql.ParseDSN(ServerDSN())
	cfg.DBName = name
	st, err := store.Open(ctx, cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		st.DB.Close()
		if db, err := sql.Open("mysql", ServerDSN()); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS " + name)
			db.Close()
		}
	})
	return st
}

// DSN of the store's database, for subprocesses.
func DSN(t testing.TB, st *store.Store) string {
	var name string
	if err := st.DB.QueryRow("SELECT DATABASE()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	cfg, _ := mysql.ParseDSN(ServerDSN())
	cfg.DBName = name
	return cfg.FormatDSN()
}

// KafkaBrokers the tests use.
func KafkaBrokers() []string {
	if v := os.Getenv("DISPATCHD_TEST_KAFKA"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:19193"}
}

// Kafka creates a fresh pair of topics for one test (a random prefix)
// and deletes them afterwards.
func Kafka(t testing.TB) (brokers []string, topics queue.Topics) {
	t.Helper()
	Integration(t)
	brokers = KafkaBrokers()
	topics = queue.Topics{Prefix: fmt.Sprintf("t%d%06d.", time.Now().Unix()%1_000_000, rand.IntN(1_000_000))}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := queue.EnsureTopics(ctx, brokers, 3, topics.All()...); err != nil {
		t.Fatalf("Kafka for integration tests is not reachable at %v (run scripts/test.sh): %v", brokers, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = queue.DeleteTopics(ctx, brokers, topics.All()...)
	})
	return brokers, topics
}
