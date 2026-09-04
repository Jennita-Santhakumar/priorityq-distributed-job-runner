package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
	"github.com/KabileshRajaselvan/task-queue-system/internal/metrics"
)

// fakeQueue is a minimal in-memory queue.Queue implementation for handler
// unit tests, so these tests don't need a real Redis instance. Handler's
// store dependency is the concrete *store.Store type (a pgx pool wrapper),
// so store-dependent paths (happy-path CreateJob, GetJob, CancelJob,
// GetStats) are exercised against a real Postgres in
// tests/integration/job_lifecycle_test.go instead of being mocked here.
// These tests cover the validation logic that runs before the store is
// ever touched.
type fakeQueue struct {
	enqueued []*job.Job
}

func (f *fakeQueue) Enqueue(ctx context.Context, j *job.Job) error {
	f.enqueued = append(f.enqueued, j)
	return nil
}
func (f *fakeQueue) Dequeue(ctx context.Context) (*job.Job, error)      { return nil, nil }
func (f *fakeQueue) Depth(ctx context.Context, t string) (int64, error) { return 0, nil }
func (f *fakeQueue) DepthByType(ctx context.Context) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (f *fakeQueue) PublishWorkerHeartbeat(ctx context.Context, n int) error { return nil }
func (f *fakeQueue) WorkerHeartbeatCount(ctx context.Context) (int, error)   { return 0, nil }
func (f *fakeQueue) Remove(ctx context.Context, jobType, jobID string) error { return nil }

func newTestHandler(t *testing.T) (*Handler, *fakeQueue) {
	t.Helper()
	registry := job.NewRegistry()
	registry.Register("data_transform", noopHandler{})

	fq := &fakeQueue{}
	m := metrics.New(prometheus.NewRegistry())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	h := &Handler{
		queue:     fq,
		registry:  registry,
		validator: validator.New(),
		logger:    logger,
		metrics:   m,
	}
	return h, fq
}

type noopHandler struct{}

func (noopHandler) Execute(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}

func TestCreateJob_InvalidBody(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString("not json"))
	rec := httptest.NewRecorder()

	h.CreateJob(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateJob_UnknownType(t *testing.T) {
	h, _ := newTestHandler(t)
	body := `{"type":"nonexistent","payload":{"a":1}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.CreateJob(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateJob_MissingType(t *testing.T) {
	h, _ := newTestHandler(t)
	body := `{"payload":{"a":1}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.CreateJob(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateJob_MissingPayload(t *testing.T) {
	h, _ := newTestHandler(t)
	body := `{"type":"data_transform"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.CreateJob(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateJob_PriorityOutOfRange(t *testing.T) {
	h, _ := newTestHandler(t)
	body := `{"type":"data_transform","payload":{"a":1},"priority":99}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.CreateJob(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}
