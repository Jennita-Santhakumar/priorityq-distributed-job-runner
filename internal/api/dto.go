package api

import (
	"encoding/json"
	"time"
)

type CreateJobRequest struct {
	Type        string          `json:"type" validate:"required,min=1,max=255"`
	Payload     json.RawMessage `json:"payload" validate:"required"`
	Priority    int             `json:"priority" validate:"min=0,max=10"`
	MaxRetries  int             `json:"max_retries" validate:"min=0,max=10"`
	ScheduledAt *time.Time      `json:"scheduled_at"`
}

type JobResponse struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error_message,omitempty"`
	Priority    int             `json:"priority"`
	MaxRetries  int             `json:"max_retries"`
	RetryCount  int             `json:"retry_count"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	ExecutionMs *int            `json:"execution_time_ms,omitempty"`
}

type JobListResponse struct {
	Jobs   []JobResponse `json:"jobs"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

type StatsResponse struct {
	PendingJobs          int              `json:"pending_jobs"`
	ProcessingJobs       int              `json:"processing_jobs"`
	CompletedJobs        int              `json:"completed_jobs"`
	FailedJobs           int              `json:"failed_jobs"`
	DeadLetterCount      int              `json:"dead_letter_count"`
	AvgProcessingTimeMs  float64          `json:"avg_processing_time_ms"`
	ThroughputJobsPerSec float64          `json:"throughput_jobs_per_sec"`
	QueueDepthByType     map[string]int64 `json:"queue_depth_by_type"`
	WorkerCount          int              `json:"worker_count"`
	UptimeSeconds        int64            `json:"uptime_seconds"`
}
