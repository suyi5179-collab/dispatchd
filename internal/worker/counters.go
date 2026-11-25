package worker

import (
	"sync/atomic"
	"time"
)

// Duplicates counts Kafka messages skipped because their attempt was
// already claimed or the notification was already finished.
var Duplicates atomic.Int64

// ObserveTokenWait records a token-bucket wait (a metric from chapter 06).
var ObserveTokenWait = func(provider string, waited time.Duration) {}
