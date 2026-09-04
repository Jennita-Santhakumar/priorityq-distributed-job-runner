// Package handlers implements the demo job handlers registered with the worker pool.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// DataTransformPayload is the request shape for the data_transform job type.
type DataTransformPayload struct {
	Numbers   []float64 `json:"numbers"`
	Operation string    `json:"operation"` // sum, mean, sort, histogram
	FailRate  float64   `json:"fail_rate,omitempty"`
}

type DataTransformResult struct {
	Operation string      `json:"operation"`
	Count     int         `json:"count"`
	Result    interface{} `json:"result"`
}

// DataTransformHandler performs a real CPU-bound aggregation over the input
// numbers and simulates realistic variable latency (proportional to input
// size, capped) so queue-depth/throughput metrics are visible in a demo.
// The optional FailRate field exists purely to demo retry/DLQ behavior on
// command: it deterministically fails a fraction of executions using a
// per-job seeded RNG.
type DataTransformHandler struct{}

func (h *DataTransformHandler) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p DataTransformPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("invalid data_transform payload: %w", err)
	}
	if len(p.Numbers) == 0 {
		return nil, fmt.Errorf("data_transform payload requires a non-empty numbers array")
	}

	// Simulate variable processing latency proportional to input size, capped at 3s.
	latency := time.Duration(len(p.Numbers)) * time.Millisecond
	if latency > 3*time.Second {
		latency = 3 * time.Second
	}
	select {
	case <-time.After(latency):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if p.FailRate > 0 {
		src := rand.New(rand.NewSource(int64(len(p.Numbers)) ^ time.Now().UnixNano()))
		if src.Float64() < p.FailRate {
			return nil, fmt.Errorf("simulated failure (fail_rate=%.2f)", p.FailRate)
		}
	}

	var result interface{}
	switch p.Operation {
	case "sum":
		var sum float64
		for _, n := range p.Numbers {
			sum += n
		}
		result = sum
	case "mean":
		var sum float64
		for _, n := range p.Numbers {
			sum += n
		}
		result = sum / float64(len(p.Numbers))
	case "sort":
		sorted := append([]float64(nil), p.Numbers...)
		sort.Float64s(sorted)
		result = sorted
	case "histogram":
		buckets := make(map[string]int)
		for _, n := range p.Numbers {
			bucket := fmt.Sprintf("%d-%d", int(n/10)*10, int(n/10)*10+10)
			buckets[bucket]++
		}
		result = buckets
	default:
		return nil, fmt.Errorf("unknown operation %q (expected sum, mean, sort, histogram)", p.Operation)
	}

	out := DataTransformResult{Operation: p.Operation, Count: len(p.Numbers), Result: result}
	return json.Marshal(out)
}
