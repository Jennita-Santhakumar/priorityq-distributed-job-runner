package store

import "context"

// DBStats holds the subset of GET /api/v1/stats fields that come from
// PostgreSQL. Queue depth by type comes from Redis (internal/queue) and
// worker_count/uptime_seconds are process-level facts the API layer knows
// directly — those are merged in by internal/api/handlers.go.
type DBStats struct {
	PendingJobs      int     `json:"pending_jobs"`
	ProcessingJobs   int     `json:"processing_jobs"`
	CompletedJobs    int     `json:"completed_jobs"`
	FailedJobs       int     `json:"failed_jobs"`
	DeadLetterCount  int     `json:"dead_letter_count"`
	AvgProcessingMs  float64 `json:"avg_processing_time_ms"`
	ThroughputPerSec float64 `json:"throughput_jobs_per_sec"`
}

func (s *Store) GetStats(ctx context.Context) (*DBStats, error) {
	var stats DBStats
	err := s.withBreaker(ctx, func() error {
		row := s.pool.QueryRow(ctx, `
			SELECT
				count(*) FILTER (WHERE status = 'pending'),
				count(*) FILTER (WHERE status = 'processing'),
				count(*) FILTER (WHERE status = 'completed'),
				count(*) FILTER (WHERE status = 'failed'),
				coalesce(avg(execution_time_ms) FILTER (WHERE status = 'completed'), 0),
				count(*) FILTER (WHERE status = 'completed' AND completed_at > now() - interval '60 seconds')::float / 60.0
			FROM jobs`)
		if err := row.Scan(
			&stats.PendingJobs, &stats.ProcessingJobs, &stats.CompletedJobs, &stats.FailedJobs,
			&stats.AvgProcessingMs, &stats.ThroughputPerSec,
		); err != nil {
			return err
		}
		return s.pool.QueryRow(ctx, `SELECT count(*) FROM dead_letter_queue`).Scan(&stats.DeadLetterCount)
	})
	return &stats, err
}
