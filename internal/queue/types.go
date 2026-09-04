package queue

import (
	"context"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
)

// Queue is the abstraction the API server and worker pool depend on, so
// neither imports the concrete Redis client directly.
type Queue interface {
	Enqueue(ctx context.Context, j *job.Job) error
	// Dequeue returns the next ready job, or (nil, nil) if none are ready.
	Dequeue(ctx context.Context) (*job.Job, error)
	Depth(ctx context.Context, jobType string) (int64, error)
	DepthByType(ctx context.Context) (map[string]int64, error)

	// Remove deletes a job from its type's sorted set. Used when a pending
	// job is cancelled: cancellation alone only updates Postgres, so
	// without this a cancelled job with a future scheduled_at would sit in
	// Redis indefinitely, inflating queue-depth metrics for a job that will
	// never execute (the worker does skip it on dequeue via a status
	// check, but it should never have stayed queued in the first place).
	Remove(ctx context.Context, jobType, jobID string) error

	// PublishWorkerHeartbeat records the current worker pool size with a
	// short TTL, so the API process (a separate process/container from the
	// worker pool) can report a live worker_count in GET /api/v1/stats
	// without the two processes sharing memory. If workers stop refreshing
	// it, it naturally expires back to unknown rather than reporting a
	// stale non-zero count forever.
	PublishWorkerHeartbeat(ctx context.Context, count int) error
	WorkerHeartbeatCount(ctx context.Context) (int, error)
}
