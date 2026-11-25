// Package delayq is a delay queue: items come out when they are due, in
// due order. It is a min-heap keyed on the due time plus one timer set to
// the top of the heap. Nothing polls: the goroutine sleeps until the
// earliest item is due, and a Push only wakes it when the new item is
// earlier than the current top.
package delayq

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Item is something to do at Due.
type Item struct {
	ID       string    `json:"id"`
	Due      time.Time `json:"due"`
	Kind     string    `json:"kind"` // "retry" or "lease"
	Attempts int       `json:"attempts"`
	index    int
}

type minHeap []*Item

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].Due.Before(h[j].Due) }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *minHeap) Push(x any)        { it := x.(*Item); it.index = len(*h); *h = append(*h, it) }
func (h *minHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	it.index = -1
	return it
}

// Queue is safe for concurrent Push; one Run goroutine pops.
type Queue struct {
	mu   sync.Mutex
	h    minHeap
	byID map[string]*Item
	kick chan struct{}

	// TimerFires counts wake-ups because the top item came due;
	// Kicks counts wake-ups because a Push put a new item on top.
	// Together they are every time the Run goroutine woke up.
	TimerFires atomic.Int64
	Kicks      atomic.Int64
}

// New makes an empty queue.
func New() *Queue {
	return &Queue{byID: map[string]*Item{}, kick: make(chan struct{}, 1)}
}

// Push adds an item, or moves it if an item with the same ID is queued
// (one pending action per notification).
// @learn:heap-push
func (q *Queue) Push(it Item) {
	q.mu.Lock()
	oldTop := time.Time{}
	if len(q.h) > 0 {
		oldTop = q.h[0].Due
	}
	if cur, ok := q.byID[it.ID]; ok {
		cur.Due, cur.Kind, cur.Attempts = it.Due, it.Kind, it.Attempts
		heap.Fix(&q.h, cur.index)
	} else {
		p := it
		heap.Push(&q.h, &p)
		q.byID[it.ID] = &p
	}
	newTop := q.h[0].Due
	q.mu.Unlock()
	// Only an earlier top changes when Run must wake up.
	if oldTop.IsZero() || newTop.Before(oldTop) {
		select {
		case q.kick <- struct{}{}:
		default:
		}
	}
}

// @learn:end heap-push

// Len is the number of queued items.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h)
}

// Peek returns the earliest item.
func (q *Queue) Peek() (Item, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.h) == 0 {
		return Item{}, false
	}
	return *q.h[0], true
}

// Run pops due items and calls fire for each, until ctx ends.
// @learn:heap-run
func (q *Queue) Run(ctx context.Context, fire func(Item)) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		q.mu.Lock()
		now := time.Now()
		var due []Item
		for len(q.h) > 0 && !q.h[0].Due.After(now) {
			it := heap.Pop(&q.h).(*Item)
			delete(q.byID, it.ID)
			due = append(due, *it)
		}
		var wait time.Duration = -1
		if len(q.h) > 0 {
			wait = q.h[0].Due.Sub(now)
		}
		q.mu.Unlock()
		for _, it := range due {
			fire(it)
		}
		if wait >= 0 {
			timer.Reset(wait) // sleep exactly until the top is due
		}
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			q.TimerFires.Add(1)
		case <-q.kick: // a new earliest item: recompute the wait
			q.Kicks.Add(1)
			timer.Stop()
		}
	}
}

// @learn:end heap-run
