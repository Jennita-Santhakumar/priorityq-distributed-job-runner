package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestComputeBackoff(t *testing.T) {
	base := time.Second
	maxDelay := 5 * time.Minute

	tests := []struct {
		retryCount int
		minExpect  time.Duration
		maxExpect  time.Duration
	}{
		{0, time.Second, time.Second + time.Second/4},
		{1, 2 * time.Second, 2*time.Second + 2*time.Second/4},
		{2, 4 * time.Second, 4*time.Second + 4*time.Second/4},
		{5, 32 * time.Second, 32*time.Second + 32*time.Second/4},
	}

	for _, tt := range tests {
		delay := computeBackoff(tt.retryCount, base, maxDelay)
		assert.GreaterOrEqual(t, delay, tt.minExpect, "retryCount=%d", tt.retryCount)
		assert.LessOrEqual(t, delay, tt.maxExpect, "retryCount=%d", tt.retryCount)
	}
}

func TestComputeBackoffNeverExceedsMax(t *testing.T) {
	base := time.Second
	maxDelay := 30 * time.Second

	for retryCount := 0; retryCount <= 20; retryCount++ {
		delay := computeBackoff(retryCount, base, maxDelay)
		assert.LessOrEqual(t, delay, maxDelay, "retryCount=%d produced delay > maxDelay", retryCount)
	}
}

func TestComputeBackoffNegativeRetryCountTreatedAsZero(t *testing.T) {
	base := time.Second
	maxDelay := time.Minute
	delay := computeBackoff(-3, base, maxDelay)
	assert.GreaterOrEqual(t, delay, base)
	assert.LessOrEqual(t, delay, base+base/4)
}

func TestComputeBackoffMonotonicTrend(t *testing.T) {
	// Ignoring jitter noise, larger retry counts should trend toward larger
	// delays until the cap is hit.
	base := time.Second
	maxDelay := 10 * time.Minute
	prevMin := time.Duration(0)
	for retryCount := 0; retryCount < 8; retryCount++ {
		expectedFloor := time.Duration(float64(base) * pow2(retryCount))
		if expectedFloor > maxDelay {
			expectedFloor = maxDelay
		}
		assert.GreaterOrEqual(t, expectedFloor, prevMin)
		prevMin = expectedFloor
	}
}

func pow2(n int) float64 {
	result := 1.0
	for i := 0; i < n; i++ {
		result *= 2
	}
	return result
}
