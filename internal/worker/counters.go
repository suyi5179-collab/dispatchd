package worker

import "sync/atomic"

// Duplicates counts Kafka messages skipped because their attempt was
// already claimed or the notification was already finished.
var Duplicates atomic.Int64
