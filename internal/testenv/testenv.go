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
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

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
