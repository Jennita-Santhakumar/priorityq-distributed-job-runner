package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-playground/validator/v10"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
	"github.com/KabileshRajaselvan/task-queue-system/internal/metrics"
	"github.com/KabileshRajaselvan/task-queue-system/internal/queue"
	"github.com/KabileshRajaselvan/task-queue-system/internal/store"
	"github.com/KabileshRajaselvan/task-queue-system/pkg/apierror"
)

type Handler struct {
	store     *store.Store
	queue     queue.Queue
	registry  *job.Registry
	validator *validator.Validate
	logger    *slog.Logger
	metrics   *metrics.Metrics
	startedAt time.Time
}

func NewHandler(s *store.Store, q queue.Queue, registry *job.Registry, m *metrics.Metrics, logger *slog.Logger) *Handler {
	return &Handler{
		store:     s,
		queue:     q,
		registry:  registry,
		validator: validator.New(),
		logger:    logger,
		metrics:   m,
		startedAt: time.Now(),
	}
}

func toJobResponse(j *job.Job) JobResponse {
	resp := JobResponse{
		ID:          j.ID,
		Type:        j.Type,
		Status:      string(j.Status),
		Payload:     j.Payload,
		Result:      j.Result,
		Priority:    j.Priority,
		MaxRetries:  j.MaxRetries,
		RetryCount:  j.RetryCount,
		CreatedAt:   j.CreatedAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		ExecutionMs: j.ExecutionTimeMs,
	}
	if j.ErrorMessage != nil {
		resp.Error = *j.ErrorMessage
	}
	return resp
}

// CreateJob handles POST /api/v1/jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeoutCtx(r)
	defer cancel()

	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.Write(w, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}
	if req.Priority == 0 {
		req.Priority = 5 // PRD default
	}
	if req.MaxRetries == 0 {
		req.MaxRetries = 3 // PRD default
	}

	if err := h.validator.Struct(req); err != nil {
		apierror.Write(w, http.StatusBadRequest, "validation_failed", err.Error())
		return
	}
	if !h.registry.Has(req.Type) {
		apierror.Write(w, http.StatusBadRequest, "unknown_job_type", "unknown job type: "+req.Type)
		return
	}

	j := &job.Job{
		Type:        req.Type,
		Payload:     req.Payload,
		Status:      job.StatusPending,
		Priority:    req.Priority,
		MaxRetries:  req.MaxRetries,
		ScheduledAt: req.ScheduledAt,
	}

	if err := h.store.CreateJob(ctx, j); err != nil {
		h.logger.Error("failed to create job", "error", err)
		apierror.Write(w, http.StatusInternalServerError, "internal_error", "failed to create job")
		return
	}

	if err := h.queue.Enqueue(ctx, j); err != nil {
		h.logger.Error("failed to enqueue job", "error", err)
		apierror.Write(w, http.StatusInternalServerError, "internal_error", "failed to enqueue job")
		return
	}

	h.metrics.JobCreated(j.Type, j.Priority)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(toJobResponse(j))
}

// GetJob handles GET /api/v1/jobs/{job_id}
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeoutCtx(r)
	defer cancel()

	jobID := chi.URLParam(r, "job_id")
	j, err := h.store.GetJob(ctx, jobID)
	if errors.Is(err, store.ErrNotFound) {
		apierror.Write(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if err != nil {
		h.logger.Error("failed to get job", "job_id", jobID, "error", err)
		apierror.Write(w, http.StatusInternalServerError, "internal_error", "failed to get job")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toJobResponse(j))
}

// ListJobs handles GET /api/v1/jobs — an addition beyond the PRD's literal
// 4 endpoints, needed so the dashboard can show recent jobs without already
// knowing their IDs. Documented in README's Design Decisions.
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeoutCtx(r)
	defer cancel()

	q := r.URL.Query()
	limit := 20
	if v, err := strconv.Atoi(q.Get("limit")); err == nil && v > 0 && v <= 200 {
		limit = v
	}
	offset := 0
	if v, err := strconv.Atoi(q.Get("offset")); err == nil && v >= 0 {
		offset = v
	}

	jobs, err := h.store.ListJobs(ctx, q.Get("status"), q.Get("type"), limit, offset)
	if err != nil {
		h.logger.Error("failed to list jobs", "error", err)
		apierror.Write(w, http.StatusInternalServerError, "internal_error", "failed to list jobs")
		return
	}

	resp := JobListResponse{Jobs: make([]JobResponse, 0, len(jobs)), Limit: limit, Offset: offset}
	for _, j := range jobs {
		resp.Jobs = append(resp.Jobs, toJobResponse(j))
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// CancelJob handles DELETE /api/v1/jobs/{job_id}
func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeoutCtx(r)
	defer cancel()

	jobID := chi.URLParam(r, "job_id")
	jobType, err := h.store.CancelJob(ctx, jobID)
	switch {
	case err == nil:
		if err := h.queue.Remove(ctx, jobType, jobID); err != nil {
			h.logger.Warn("cancelled job left queued in redis", "job_id", jobID, "error", err)
		}
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, store.ErrNotFound):
		apierror.Write(w, http.StatusNotFound, "not_found", "job not found")
	case errors.Is(err, store.ErrCannotCancel):
		apierror.Write(w, http.StatusConflict, "cannot_cancel", "job already picked up or completed")
	default:
		h.logger.Error("failed to cancel job", "job_id", jobID, "error", err)
		apierror.Write(w, http.StatusInternalServerError, "internal_error", "failed to cancel job")
	}
}

// GetStats handles GET /api/v1/stats
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeoutCtx(r)
	defer cancel()

	dbStats, err := h.store.GetStats(ctx)
	if err != nil {
		h.logger.Error("failed to get stats", "error", err)
		apierror.Write(w, http.StatusInternalServerError, "internal_error", "failed to get stats")
		return
	}
	depthByType, err := h.queue.DepthByType(ctx)
	if err != nil {
		h.logger.Error("failed to get queue depth", "error", err)
		depthByType = map[string]int64{}
	}
	workerCount, err := h.queue.WorkerHeartbeatCount(ctx)
	if err != nil {
		workerCount = 0
	}

	resp := StatsResponse{
		PendingJobs:          dbStats.PendingJobs,
		ProcessingJobs:       dbStats.ProcessingJobs,
		CompletedJobs:        dbStats.CompletedJobs,
		FailedJobs:           dbStats.FailedJobs,
		DeadLetterCount:      dbStats.DeadLetterCount,
		AvgProcessingTimeMs:  dbStats.AvgProcessingMs,
		ThroughputJobsPerSec: dbStats.ThroughputPerSec,
		QueueDepthByType:     depthByType,
		WorkerCount:          workerCount,
		UptimeSeconds:        int64(time.Since(h.startedAt).Seconds()),
	}

	for t, d := range depthByType {
		h.metrics.SetQueueDepth(t, d)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// Health handles GET /health
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := timeoutCtx(r)
	defer cancel()
	if err := h.store.Ping(ctx); err != nil {
		apierror.Write(w, http.StatusServiceUnavailable, "unhealthy", "database unreachable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func timeoutCtx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 5*time.Second)
}
