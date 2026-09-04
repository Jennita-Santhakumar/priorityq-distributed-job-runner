package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
)

func workerID(id int) string {
	return fmt.Sprintf("worker-%d-%d", os.Getpid(), id)
}

// executeJob fetches the full job from Postgres (Dequeue only carries the ID
// and type — Postgres, not Redis, is the source of truth for job state),
// marks it processing, runs the registered handler under a bounded timeout,
// and settles the outcome.
func (p *Pool) executeJob(ctx context.Context, workerNum int, ref *job.Job) {
	wid := workerID(workerNum)

	full, err := p.s.GetJob(ctx, ref.ID)
	if err != nil {
		p.logger.Error("failed to load job for execution", "job_id", ref.ID, "error", err)
		return
	}
	// A job can be dequeued after it was already cancelled (pending -> cancelled
	// race) or already handled by a reclaim pass; skip anything not pending.
	if full.Status != job.StatusPending && full.Status != job.StatusRetried {
		return
	}

	if err := p.s.UpdateJobProcessing(ctx, full.ID, wid); err != nil {
		p.logger.Error("failed to mark job processing", "job_id", full.ID, "error", err)
		return
	}
	_ = p.s.RecordHistory(ctx, full.ID, string(job.StatusProcessing), "picked up by worker", wid)

	handler, ok := p.registry.Get(full.Type)
	if !ok {
		p.handleError(ctx, full, wid, fmt.Sprintf("unknown job type %q", full.Type))
		return
	}

	execCtx, cancel := context.WithTimeout(ctx, p.cfg.JobExecTimeout)
	defer cancel()

	start := time.Now()
	result, err := handler.Execute(execCtx, full.Payload)
	durationMs := int(time.Since(start).Milliseconds())

	if err != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("job execution timed out after %s: %w", p.cfg.JobExecTimeout, err)
		}
		p.handleFailure(ctx, full, wid, err)
		return
	}
	p.handleSuccess(ctx, full, wid, result, durationMs)
}

func (p *Pool) handleSuccess(ctx context.Context, j *job.Job, wid string, result []byte, durationMs int) {
	if err := p.s.UpdateJobCompleted(ctx, j.ID, result, durationMs); err != nil {
		p.logger.Error("failed to mark job completed", "job_id", j.ID, "error", err)
		return
	}
	_ = p.s.RecordHistory(ctx, j.ID, string(job.StatusCompleted), "job succeeded", wid)
	p.metrics.JobCompleted(j.Type, durationMs)
	p.logger.Info("job completed", "job_id", j.ID, "type", j.Type, "duration_ms", durationMs)
}

// handleFailure routes a failed execution to either a backoff retry or, if
// retries are exhausted, permanent failure + DLQ.
func (p *Pool) handleFailure(ctx context.Context, j *job.Job, wid string, execErr error) {
	nextRetryCount := j.RetryCount + 1

	if nextRetryCount > j.MaxRetries {
		p.handleError(ctx, j, wid, execErr.Error())
		return
	}

	delay := computeBackoff(nextRetryCount, p.cfg.BackoffBase, p.cfg.BackoffMax)
	scheduledAt := time.Now().Add(delay)

	if err := p.s.UpdateJobRetried(ctx, j.ID, nextRetryCount, scheduledAt, execErr.Error()); err != nil {
		p.logger.Error("failed to mark job retried", "job_id", j.ID, "error", err)
		return
	}

	retryJob := *j
	retryJob.ScheduledAt = &scheduledAt
	if err := p.q.Enqueue(ctx, &retryJob); err != nil {
		p.logger.Error("failed to re-enqueue retried job", "job_id", j.ID, "error", err)
		return
	}

	_ = p.s.RecordHistory(ctx, j.ID, string(job.StatusRetried),
		fmt.Sprintf("retry %d/%d in %s: %s", nextRetryCount, j.MaxRetries, delay, execErr.Error()), wid)
	p.metrics.JobRetried(j.Type)
	p.logger.Warn("job scheduled for retry", "job_id", j.ID, "retry", nextRetryCount, "delay", delay, "error", execErr)
}

func (p *Pool) handleError(ctx context.Context, j *job.Job, wid, errMsg string) {
	if err := p.s.UpdateJobFailed(ctx, j.ID, errMsg); err != nil {
		p.logger.Error("failed to mark job failed", "job_id", j.ID, "error", err)
		return
	}
	if err := p.s.MoveToDLQ(ctx, j.ID, errMsg); err != nil {
		p.logger.Error("failed to move job to DLQ", "job_id", j.ID, "error", err)
	}
	_ = p.s.RecordHistory(ctx, j.ID, string(job.StatusFailed), errMsg, wid)
	p.metrics.JobFailed(j.Type)
	p.logger.Error("job failed permanently", "job_id", j.ID, "error", errMsg)
}

// runStaleReclaim periodically resets jobs stuck in 'processing' (a worker
// crashed mid-execution without ever settling the job) back to 'pending'
// and re-enqueues them — the concrete mechanism behind the PRD's own
// error-handling table entry for worker crashes, which the PRD describes
// but never implements.
func (p *Pool) runStaleReclaim(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.StaleReclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reclaimed, err := p.s.ReclaimStaleProcessingJobs(ctx, int(p.cfg.StaleReclaimAfter.Minutes()))
			if err != nil {
				p.logger.Error("stale job reclaim failed", "error", err)
				continue
			}
			for _, j := range reclaimed {
				if err := p.q.Enqueue(ctx, j); err != nil {
					p.logger.Error("failed to re-enqueue reclaimed job", "job_id", j.ID, "error", err)
					continue
				}
				_ = p.s.RecordHistory(ctx, j.ID, string(job.StatusPending), "reclaimed from stale processing state", "")
				p.logger.Warn("reclaimed stale job", "job_id", j.ID, "type", j.Type)
			}
		}
	}
}
