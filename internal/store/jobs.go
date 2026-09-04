package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
)

var (
	ErrNotFound     = errors.New("job not found")
	ErrCannotCancel = errors.New("job cannot be cancelled: already picked up or terminal")
)

const jobSelectColumns = `id, type, payload, status, result, error_message, priority,
	max_retries, retry_count, created_at, updated_at, started_at, completed_at,
	scheduled_at, worker_id, execution_time_ms`

// CreateJob inserts a new job row. The id, created_at, updated_at fields on
// the passed Job are populated with the persisted database's own values.
func (s *Store) CreateJob(ctx context.Context, j *job.Job) error {
	return s.withBreaker(ctx, func() error {
		row := s.pool.QueryRow(ctx, `
			INSERT INTO jobs (type, payload, status, priority, max_retries, scheduled_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id, created_at, updated_at`,
			j.Type, j.Payload, j.Status, j.Priority, j.MaxRetries, j.ScheduledAt,
		)
		return row.Scan(&j.ID, &j.CreatedAt, &j.UpdatedAt)
	})
}

func scanJob(row pgx.Row) (*job.Job, error) {
	var j job.Job
	var result []byte
	var payload []byte
	err := row.Scan(
		&j.ID, &j.Type, &payload, &j.Status, &result, &j.ErrorMessage, &j.Priority,
		&j.MaxRetries, &j.RetryCount, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt,
		&j.ScheduledAt, &j.WorkerID, &j.ExecutionTimeMs,
	)
	if err != nil {
		return nil, err
	}
	j.Payload = json.RawMessage(payload)
	if result != nil {
		j.Result = json.RawMessage(result)
	}
	return &j, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (*job.Job, error) {
	var result *job.Job
	err := s.withBreaker(ctx, func() error {
		row := s.pool.QueryRow(ctx, `SELECT `+jobSelectColumns+` FROM jobs WHERE id = $1`, id)
		j, err := scanJob(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		result = j
		return nil
	})
	return result, err
}

// ListJobs is the list endpoint the PRD never specifies but the dashboard
// needs to show "recent jobs" without already knowing job IDs.
func (s *Store) ListJobs(ctx context.Context, status, jobType string, limit, offset int) ([]*job.Job, error) {
	var results []*job.Job
	err := s.withBreaker(ctx, func() error {
		rows, err := s.pool.Query(ctx, `
			SELECT `+jobSelectColumns+` FROM jobs
			WHERE ($1 = '' OR status = $1) AND ($2 = '' OR type = $2)
			ORDER BY created_at DESC
			LIMIT $3 OFFSET $4`,
			status, jobType, limit, offset,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			j, err := scanJob(rows)
			if err != nil {
				return err
			}
			results = append(results, j)
		}
		return rows.Err()
	})
	return results, err
}

// UpdateJobProcessing marks a job as picked up by a worker.
func (s *Store) UpdateJobProcessing(ctx context.Context, id, workerID string) error {
	return s.withBreaker(ctx, func() error {
		_, err := s.pool.Exec(ctx, `
			UPDATE jobs SET status = 'processing', worker_id = $2, started_at = now()
			WHERE id = $1`, id, workerID)
		return err
	})
}

// UpdateJobCompleted marks a job completed with its result.
func (s *Store) UpdateJobCompleted(ctx context.Context, id string, result json.RawMessage, executionTimeMs int) error {
	return s.withBreaker(ctx, func() error {
		_, err := s.pool.Exec(ctx, `
			UPDATE jobs SET status = 'completed', result = $2, completed_at = now(), execution_time_ms = $3
			WHERE id = $1`, id, result, executionTimeMs)
		return err
	})
}

// UpdateJobRetried marks a job for retry: bumps retry_count, schedules a
// future re-delivery time, and returns it to 'retried' status. The actual
// re-enqueue into Redis is done by the caller (internal/worker) using the
// same ScheduledAt value returned by the backoff calculation.
func (s *Store) UpdateJobRetried(ctx context.Context, id string, retryCount int, scheduledAt time.Time, errMsg string) error {
	return s.withBreaker(ctx, func() error {
		_, err := s.pool.Exec(ctx, `
			UPDATE jobs SET status = 'retried', retry_count = $2, scheduled_at = $3, error_message = $4
			WHERE id = $1`, id, retryCount, scheduledAt, errMsg)
		return err
	})
}

// UpdateJobFailed marks a job permanently failed.
func (s *Store) UpdateJobFailed(ctx context.Context, id, errMsg string) error {
	return s.withBreaker(ctx, func() error {
		_, err := s.pool.Exec(ctx, `
			UPDATE jobs SET status = 'failed', error_message = $2, completed_at = now()
			WHERE id = $1`, id, errMsg)
		return err
	})
}

// ReclaimStaleProcessingJobs resets jobs stuck in 'processing' whose
// updated_at is older than `olderThanMinutes` back to 'pending', returning
// their ids and types so the caller can re-enqueue them into Redis. This
// implements the PRD's own error-handling table entry ("Worker crash during
// execution -> job stays in processing, timeout after 30min, auto-retry"),
// which the PRD describes but never actually wires into code.
func (s *Store) ReclaimStaleProcessingJobs(ctx context.Context, olderThanMinutes int) ([]*job.Job, error) {
	var results []*job.Job
	err := s.withBreaker(ctx, func() error {
		rows, err := s.pool.Query(ctx, `
			UPDATE jobs SET status = 'pending', worker_id = NULL
			WHERE status = 'processing' AND updated_at < now() - make_interval(mins => $1)
			RETURNING `+jobSelectColumns,
			olderThanMinutes,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			j, err := scanJob(rows)
			if err != nil {
				return err
			}
			results = append(results, j)
		}
		return rows.Err()
	})
	return results, err
}

// CancelJob atomically cancels a job only if it is still pending, avoiding
// the read-then-write race the PRD's DELETE endpoint spec implies but never
// guards against. Returns ErrNotFound if no such job exists at all, or
// ErrCannotCancel if it exists but is no longer pending (already picked up
// or terminal) — the API layer maps these to 404 and 409 respectively.
// CancelJob returns the job's type on success, so the caller can also
// remove it from the Redis queue (cancellation alone only updates
// Postgres — without this, a cancelled job with a far-future scheduled_at
// would sit in the Redis sorted set indefinitely, inflating queue-depth
// metrics even though it will never execute).
func (s *Store) CancelJob(ctx context.Context, id string) (string, error) {
	var jobType string
	err := s.withBreaker(ctx, func() error {
		var cancelledID string
		row := s.pool.QueryRow(ctx, `
			UPDATE jobs SET status = 'cancelled', completed_at = now()
			WHERE id = $1 AND status = 'pending'
			RETURNING id, type`, id)
		err := row.Scan(&cancelledID, &jobType)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		// Zero rows affected: distinguish "does not exist" from "exists but not pending".
		var exists bool
		checkErr := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)`, id).Scan(&exists)
		if checkErr != nil {
			return checkErr
		}
		if !exists {
			return ErrNotFound
		}
		return ErrCannotCancel
	})
	return jobType, err
}

func (s *Store) CountJobsByStatus(ctx context.Context, status string) (int, error) {
	var count int
	err := s.withBreaker(ctx, func() error {
		return s.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = $1`, status).Scan(&count)
	})
	return count, err
}
