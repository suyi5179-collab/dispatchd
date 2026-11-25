package delayq

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"
)

type sink struct {
	mu  sync.Mutex
	got []Item
	at  []time.Time
}

func (s *sink) fire(it Item) {
	s.mu.Lock()
	s.got = append(s.got, it)
	s.at = append(s.at, time.Now())
	s.mu.Unlock()
}

func (s *sink) n() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.got) }

func start(t *testing.T) (*Queue, *sink) {
	q, s := New(), &sink{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.Run(ctx, s.fire)
	return q, s
}

func waitN(t *testing.T, s *sink, n int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for s.n() < n {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d items fired after %v", s.n(), n, d)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestShuffledInsertsComeOutInDueOrder(t *testing.T) {
	q, s := start(t)
	base := time.Now().Add(50 * time.Millisecond)
	idx := rand.Perm(200)
	for _, i := range idx {
		q.Push(Item{ID: strconv.Itoa(i), Due: base.Add(time.Duration(i) * time.Millisecond)})
	}
	waitN(t, s, 200, 3*time.Second)
	for i, it := range s.got {
		if it.ID != strconv.Itoa(i) {
			t.Fatalf("position %d: got %s", i, it.ID)
		}
		if s.at[i].Before(it.Due) {
			t.Fatalf("%s fired %v early", it.ID, it.Due.Sub(s.at[i]))
		}
	}
	// 200 items over 200 ms: the goroutine woke at most once per item
	// (fewer when several were due together), never more.
	if f := q.TimerFires.Load(); f > 200 {
		t.Fatalf("%d timer wake-ups for 200 items", f)
	}
}

// @learn:heap-tests
func TestEarlierItemWakesTheQueueEarlier(t *testing.T) {
	q, s := start(t)
	q.Push(Item{ID: "late", Due: time.Now().Add(time.Second)})
	t0 := time.Now()
	q.Push(Item{ID: "early", Due: t0.Add(50 * time.Millisecond)})
	waitN(t, s, 1, 500*time.Millisecond)
	if s.got[0].ID != "early" {
		t.Fatalf("first out: %s", s.got[0].ID)
	}
	if late := s.at[0].Sub(t0); late > 150*time.Millisecond {
		t.Fatalf("early item fired after %v (the timer was still set for the late one)", late)
	}
	if q.Len() != 1 {
		t.Fatalf("queue has %d items, want the late one", q.Len())
	}
}

func TestNoBusyWaiting(t *testing.T) {
	q, s := start(t)
	time.Sleep(100 * time.Millisecond) // empty: must not wake at all
	if f, k := q.TimerFires.Load(), q.Kicks.Load(); f+k != 0 {
		t.Fatalf("empty queue woke %d times", f+k)
	}
	q.Push(Item{ID: "a", Due: time.Now().Add(300 * time.Millisecond)}) // one kick
	time.Sleep(200 * time.Millisecond)                                 // waiting: no wake-ups
	if f := q.TimerFires.Load(); f != 0 {
		t.Fatalf("woke %d times before the item was due", f)
	}
	// A later item does not change the top, so it must not wake Run.
	q.Push(Item{ID: "b", Due: time.Now().Add(time.Hour)})
	waitN(t, s, 1, time.Second)
	if f, k := q.TimerFires.Load(), q.Kicks.Load(); f != 1 || k != 1 {
		t.Fatalf("wake-ups: %d timer, %d kicks; want exactly 1 and 1", f, k)
	}
}

// @learn:end heap-tests

func TestPushSameIDMovesTheItem(t *testing.T) {
	q, s := start(t)
	q.Push(Item{ID: "x", Due: time.Now().Add(time.Hour)})
	q.Push(Item{ID: "x", Due: time.Now().Add(20 * time.Millisecond)})
	waitN(t, s, 1, time.Second)
	time.Sleep(50 * time.Millisecond)
	if s.n() != 1 || q.Len() != 0 {
		t.Fatalf("fired %d, queued %d; want 1 and 0", s.n(), q.Len())
	}
}
