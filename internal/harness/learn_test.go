package harness_test

import (
	"testing"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/harness"
)

// The learn page's backend: one real sample goes through every stage and
// each stage carries the row or event it is based on.
func TestLearnSampleWalksEveryStage(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	code, out, _ := s.Do("POST", "/v1/learn/sample", nil, "")
	if code != 200 || out["status_code"].(float64) != 202 {
		t.Fatalf("sample: %d %v", code, out)
	}
	id := out["response"].(map[string]any)["id"].(string)
	stages := func() []any {
		_, run, _ := s.Do("GET", "/v1/learn/runs/"+id, nil, "")
		return run["stages"].([]any)
	}
	deadline := time.Now().Add(20 * time.Second)
	for stages()[8].(map[string]any)["status"] != "done" {
		if time.Now().After(deadline) {
			t.Fatalf("not delivered: %v", stages())
		}
		time.Sleep(50 * time.Millisecond)
	}
	code, rep, _ := s.Do("POST", "/v1/learn/runs/"+id+"/replay", nil, "")
	if code != 200 || rep["replayed"] != true || rep["response"].(map[string]any)["id"] != id {
		t.Fatalf("replay: %d %v", code, rep)
	}
	st := stages()
	for i, x := range st {
		m := x.(map[string]any)
		if m["status"] != "done" || m["data"] == nil {
			t.Fatalf("stage %d %v: %v", i, m["id"], m["status"])
		}
	}
	a1 := st[5].(map[string]any)["data"].(map[string]any)
	if a1["http_status"].(float64) != 503 {
		t.Fatalf("first attempt: %v", a1)
	}
	if b := st[6].(map[string]any)["data"].(map[string]any)["backoff"].(map[string]any); b["jitter"] != "full" {
		t.Fatalf("backoff %v", b)
	}
	rp := st[9].(map[string]any)["data"].(map[string]any)
	if rp["provider_deliveries"].(float64) != 1 || rp["rows_with_this_key"].(float64) != 1 {
		t.Fatalf("replay stage %v", rp)
	}
}
