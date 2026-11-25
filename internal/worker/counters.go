package worker

import (
	"sync/atomic"
	"time"

	"github.com/suyi5179-collab/dispatchd/internal/metrics"
)

// Duplicates counts Kafka messages skipped because their attempt was
// already claimed or the notification was already finished.
var Duplicates atomic.Int64

// ObserveTokenWait records a token-bucket wait.
func ObserveTokenWait(provider string, waited time.Duration) {
	metrics.TokenWait.WithLabelValues(provider).Observe(waited.Seconds())
}
