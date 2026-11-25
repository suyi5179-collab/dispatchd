package harness_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/fakeprovider"
	"github.com/suyi5179-collab/dispatchd/internal/harness"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// buildWorker compiles cmd/worker once per test binary.
func buildWorker(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "worker")
	out, err := exec.Command("go", "build", "-o", bin, "../../cmd/worker").CombinedOutput()
	if err != nil {
		t.Fatalf("go build worker: %v\n%s", err, out)
	}
	return bin
}

func startWorkerProcess(t *testing.T, bin string, s *harness.Stack, name string) *exec.Cmd {
	t.Helper()
	logf, _ := os.Create(filepath.Join(t.TempDir(), name+".log"))
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"DISPATCHD_MYSQL_DSN="+s.DSN,
		"DISPATCHD_KAFKA_BROKERS="+s.Brokers[0],
		"DISPATCHD_TOPIC_PREFIX="+s.Topics.Prefix,
		"DISPATCHD_GROUP="+s.Group,
		"DISPATCHD_EMAIL_URL="+s.EmailURL,
		"DISPATCHD_PUSH_URL="+s.PushURL,
		"DISPATCHD_LEASE=2s",
		"DISPATCHD_SESSION_TIMEOUT=6s",
		"DISPATCHD_CONCURRENCY=8",
		"DISPATCHD_WORKER_ADDR=127.0.0.1:0",
		"DISPATCHD_WORKER_NAME="+name,
	)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
			}
		}
		logf.Close()
	})
	return cmd
}

// A worker process is SIGKILLed in the middle of a batch: some attempts
// are in flight, some are delivered but their Kafka offsets are not
// committed. A new worker takes over. Every notification must be
// delivered, and no person may get the same message twice.
// @learn:restart-test
func TestWorkerKilledMidBatchLosesNothingAndSendsNothingTwice(t *testing.T) {
	const n = 300
	s := harness.Start(t, harness.Options{NoWorkers: true})
	bin := buildWorker(t)
	s.Email.SetMode(fakeprovider.Mode{Mode: "slow", SlowMS: 40})
	for i := range n {
		s.Submit(map[string]any{"channel": "email", "recipient": fmt.Sprintf("user%d@example.com", i), "template": "welcome", "params": map[string]any{"name": i}})
	}
	a := startWorkerProcess(t, bin, s, "worker-a")
	deadline := time.Now().Add(30 * time.Second)
	for len(s.Email.Records()) < n/4 {
		if time.Now().After(deadline) {
			t.Fatalf("worker-a delivered only %d", len(s.Email.Records()))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := a.Process.Kill(); err != nil { // SIGKILL: no cleanup, no commit
		t.Fatal(err)
	}
	_ = a.Wait()
	atKill := len(s.Email.Records())
	inflight := s.Counts()[store.StatusSending]

	startWorkerProcess(t, bin, s, "worker-b")
	counts := s.WaitSettled(60 * time.Second)

	recs := s.Email.Records()
	perID := map[string]int{}
	for _, r := range recs {
		if r.Status == 200 && !r.Duplicate {
			perID[r.NotificationID]++
		}
	}
	twice := 0
	for _, c := range perID {
		if c > 1 {
			twice++
		}
	}
	var skipped, expired int
	_ = s.Store.DB.QueryRow(`SELECT COUNT(*) FROM notification_events WHERE stage='duplicate_skipped'`).Scan(&skipped)
	_ = s.Store.DB.QueryRow(`SELECT COUNT(*) FROM notification_events WHERE stage='lease_expired'`).Scan(&expired)
	st := fakeprovider.ComputeStats(recs, 100)
	t.Logf("killed worker-a after %d provider requests with %d attempts in flight", atKill, inflight)
	t.Logf("final statuses %v; provider requests %d, delivered %d distinct, collapsed by provider key %d", counts, st.Total, len(perID), st.Duplicates)
	t.Logf("Kafka redeliveries skipped by the claim: %d; leases expired and retried: %d", skipped, expired)
	if counts[store.StatusDelivered] != n {
		t.Fatalf("delivered %d of %d", counts[store.StatusDelivered], n)
	}
	if len(perID) != n || twice != 0 {
		t.Fatalf("provider delivered %d distinct notifications, %d of them more than once", len(perID), twice)
	}
}

// @learn:end restart-test
