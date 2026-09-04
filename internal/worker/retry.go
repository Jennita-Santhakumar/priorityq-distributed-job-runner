package worker

import (
	"math"
	"math/rand"
	"time"
)

// computeBackoff returns the delay before the next retry attempt, using
// exponential growth with decorrelated jitter, capped at maxDelay.
//
// This deliberately does not use cenkalti/backoff's blocking Retry() loop
// (the PRD's own sketch does) — a blocking retry holds an entire worker
// goroutine hostage for the whole backoff duration. Instead this pure
// function computes a delay once; the caller re-enqueues the job into Redis
// with a future score and the worker goroutine returns immediately to
// polling for other work.
func computeBackoff(retryCount int, baseDelay, maxDelay time.Duration) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}
	raw := float64(baseDelay) * math.Pow(2, float64(retryCount))
	if raw > float64(maxDelay) {
		raw = float64(maxDelay)
	}
	jitter := rand.Float64() * raw / 4 // decorrelated jitter, up to 25% of the raw delay
	delay := time.Duration(raw + jitter)
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}
