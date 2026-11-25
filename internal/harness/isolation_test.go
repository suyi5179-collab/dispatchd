package harness_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/harness"
	"github.com/suyi5179-collab/dispatchd/internal/store"
)

// Email is throttled hard (5 a second) and has a backlog of 40; push has
// room. Push notifications submitted at the same time must not wait
// behind the email backlog: one topic per channel is only worth having if
// the channels are consumed independently.
// @learn:isolation-test
func TestEmailBacklogDoesNotDelayPush(t *testing.T) {
	s := harness.Start(t, harness.Options{NoWorkers: true, Limits: map[string][2]float64{"email": {5, 1}, "push": {200, 20}}})
	for i := range 40 {
		s.Submit(email(i))
	}
	var push []string
	for i := range 20 {
		push = append(push, s.Submit(map[string]any{"channel": "push", "recipient": fmt.Sprintf("device-%d", i), "template": "otp", "params": map[string]any{"code": i}}))
	}
	t0 := time.Now()
	s.StartWorker("w1")
	for _, id := range push {
		s.WaitStatus(id, store.StatusDelivered, 30*time.Second)
	}
	pushDone := time.Since(t0)
	var emailsLeft int
	_ = s.Store.DB.QueryRow(`SELECT COUNT(*) FROM notifications WHERE channel='email' AND status<>'DELIVERED'`).Scan(&emailsLeft)
	t.Logf("all 20 push delivered %v after the worker started; %d of 40 emails still waiting for tokens", pushDone.Round(10*time.Millisecond), emailsLeft)
	if pushDone > 500*time.Millisecond {
		t.Fatalf("push waited %v behind the email backlog", pushDone)
	}
}

// @learn:end isolation-test
