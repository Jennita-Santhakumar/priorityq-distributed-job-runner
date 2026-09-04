package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// MoveToDLQ records a permanently-failed job in the dead letter queue.
// The jobs row itself is already marked 'failed' by UpdateJobFailed; this
// is the separate audit record the PRD's schema calls for.
func (s *Store) MoveToDLQ(ctx context.Context, jobID, reason string) error {
	return s.withBreaker(ctx, func() error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO dead_letter_queue (job_id, reason) VALUES ($1, $2)`,
			jobID, reason,
		)
		return err
	})
}

type DLQEntry struct {
	ID       string `json:"id"`
	JobID    string `json:"job_id"`
	Reason   string `json:"reason"`
	FailedAt string `json:"failed_at"`
}

func (s *Store) GetDLQEntry(ctx context.Context, jobID string) (*DLQEntry, error) {
	var e DLQEntry
	err := s.withBreaker(ctx, func() error {
		row := s.pool.QueryRow(ctx, `
			SELECT id, job_id, coalesce(reason, ''), failed_at::text
			FROM dead_letter_queue WHERE job_id = $1 ORDER BY failed_at DESC LIMIT 1`, jobID)
		err := row.Scan(&e.ID, &e.JobID, &e.Reason, &e.FailedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func (s *Store) CountDLQ(ctx context.Context) (int, error) {
	var count int
	err := s.withBreaker(ctx, func() error {
		return s.pool.QueryRow(ctx, `SELECT count(*) FROM dead_letter_queue`).Scan(&count)
	})
	return count, err
}
