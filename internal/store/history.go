package store

import "context"

// RecordHistory appends an audit-trail row for a job status transition.
func (s *Store) RecordHistory(ctx context.Context, jobID, status, message, workerID string) error {
	return s.withBreaker(ctx, func() error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO job_history (job_id, status, message, worker_id)
			VALUES ($1, $2, $3, NULLIF($4, ''))`,
			jobID, status, message, workerID,
		)
		return err
	})
}

type HistoryEntry struct {
	Status     string `json:"status"`
	Message    string `json:"message"`
	RecordedAt string `json:"recorded_at"`
	WorkerID   string `json:"worker_id,omitempty"`
}

func (s *Store) ListHistory(ctx context.Context, jobID string) ([]HistoryEntry, error) {
	var results []HistoryEntry
	err := s.withBreaker(ctx, func() error {
		rows, err := s.pool.Query(ctx, `
			SELECT status, coalesce(message, ''), recorded_at::text, coalesce(worker_id, '')
			FROM job_history WHERE job_id = $1 ORDER BY recorded_at ASC`, jobID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e HistoryEntry
			if err := rows.Scan(&e.Status, &e.Message, &e.RecordedAt, &e.WorkerID); err != nil {
				return err
			}
			results = append(results, e)
		}
		return rows.Err()
	})
	return results, err
}
