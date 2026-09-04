// Package worker implements the worker pool: goroutines that dequeue jobs,
// execute them via registered handlers, and settle their outcome
// (completed / retried-with-backoff / failed-into-DLQ) in Postgres.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
	"github.com/KabileshRajaselvan/task-queue-system/internal/metrics"
	"github.com/KabileshRajaselvan/task-queue-system/internal/queue"
	"github.com/KabileshRajaselvan/task-queue-system/internal/store"
)

type Config struct {
	Size                 int
	JobExecTimeout       time.Duration
	ShutdownGrace        time.Duration
	StaleReclaimAfter    time.Duration
	StaleReclaimInterval time.Duration
	BackoffBase          time.Duration
	BackoffMax           time.Duration
}

type Pool struct {
	cfg      Config
	q        queue.Queue
	s        *store.Store
	registry *job.Registry
	metrics  *metrics.Metrics
	logger   *slog.Logger

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewPool(cfg Config, q queue.Queue, s *store.Store, registry *job.Registry, m *metrics.Metrics, logger *slog.Logger) *Pool {
	return &Pool{cfg: cfg, q: q, s: s, registry: registry, metrics: m, logger: logger}
}

// Start launches cfg.Size worker goroutines plus one stale-job reclaim
// goroutine, all bound to a context derived from ctx.
func (p *Pool) Start(ctx context.Context) {
	workerCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for i := 0; i < p.cfg.Size; i++ {
		p.wg.Add(1)
		go p.runWorker(workerCtx, i)
	}
	p.metrics.SetWorkerCount(p.cfg.Size)

	p.wg.Add(1)
	go p.runStaleReclaim(workerCtx)

	p.wg.Add(1)
	go p.runHeartbeat(workerCtx)
}

func (p *Pool) runHeartbeat(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	publish := func() {
		if err := p.q.PublishWorkerHeartbeat(ctx, p.cfg.Size); err != nil {
			p.logger.Warn("failed to publish worker heartbeat", "error", err)
		}
	}
	publish()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish()
		}
	}
}

// Stop signals all worker goroutines to stop pulling new jobs and waits
// (bounded by cfg.ShutdownGrace) for in-flight jobs to finish. Anything
// still running past the grace period is left in 'processing' for the
// next stale-job reclaim pass rather than forcibly killed mid-write.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("worker pool stopped cleanly")
	case <-time.After(p.cfg.ShutdownGrace):
		p.logger.Warn("worker pool shutdown grace period elapsed; some jobs may still be in-flight")
	}
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	idleBackoff := 20 * time.Millisecond
	const maxIdleBackoff = 250 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("worker stopped", "worker_id", id)
			return
		default:
		}

		j, err := p.q.Dequeue(ctx)
		if err != nil {
			p.logger.Error("dequeue failed", "worker_id", id, "error", err)
			time.Sleep(idleBackoff)
			continue
		}
		if j == nil {
			select {
			case <-time.After(idleBackoff):
			case <-ctx.Done():
				return
			}
			idleBackoff *= 2
			if idleBackoff > maxIdleBackoff {
				idleBackoff = maxIdleBackoff
			}
			continue
		}

		idleBackoff = 20 * time.Millisecond
		p.executeJob(ctx, id, j)
	}
}
