package job

import (
	"context"
	"encoding/json"
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusRetried    Status = "retried"
	StatusCancelled  Status = "cancelled"
)

// IsValidTransition reports whether a job may move from one status to another.
// Kept as a pure function so it can be unit tested without a database.
func IsValidTransition(from, to Status) bool {
	switch from {
	case StatusPending:
		return to == StatusProcessing || to == StatusCancelled
	case StatusProcessing:
		return to == StatusCompleted || to == StatusFailed || to == StatusRetried
	case StatusRetried:
		return to == StatusProcessing || to == StatusPending
	case StatusCompleted, StatusFailed, StatusCancelled:
		return false
	default:
		return false
	}
}

type Job struct {
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Status          Status          `json:"status"`
	Result          json.RawMessage `json:"result,omitempty"`
	ErrorMessage    *string         `json:"error_message,omitempty"`
	Priority        int             `json:"priority"`
	MaxRetries      int             `json:"max_retries"`
	RetryCount      int             `json:"retry_count"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	ScheduledAt     *time.Time      `json:"scheduled_at,omitempty"`
	WorkerID        *string         `json:"worker_id,omitempty"`
	ExecutionTimeMs *int            `json:"execution_time_ms,omitempty"`
}

// Handler executes a job's payload and returns its result.
type Handler interface {
	Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)
}
