// Package metrics defines the Prometheus metrics exported by the API server
// and worker pool.
package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	JobsCreated   *prometheus.CounterVec
	JobsCompleted *prometheus.CounterVec
	JobsFailed    *prometheus.CounterVec
	JobsRetried   *prometheus.CounterVec
	// DLQTotal is a metric the PRD's own checklist implies ("DLQ monitoring
	// dashboard") but its metrics.go sketch never defines.
	DLQTotal       prometheus.Counter
	ProcessingTime *prometheus.HistogramVec
	QueueDepth     *prometheus.GaugeVec
	WorkerCount    prometheus.Gauge
}

func New(registerer prometheus.Registerer) *Metrics {
	m := &Metrics{
		JobsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_queue_jobs_created_total",
			Help: "Total number of jobs created",
		}, []string{"job_type", "priority"}),
		JobsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_queue_jobs_completed_total",
			Help: "Total number of jobs completed successfully",
		}, []string{"job_type"}),
		JobsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_queue_jobs_failed_total",
			Help: "Total number of jobs failed permanently",
		}, []string{"job_type"}),
		JobsRetried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_queue_jobs_retried_total",
			Help: "Total number of job retry attempts",
		}, []string{"job_type"}),
		DLQTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_queue_dlq_total",
			Help: "Total number of jobs moved to the dead letter queue",
		}),
		ProcessingTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_queue_processing_seconds",
			Help:    "Job processing time in seconds",
			Buckets: []float64{.1, .5, 1, 5, 10, 30, 60},
		}, []string{"job_type"}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "task_queue_depth",
			Help: "Current queue depth by job type",
		}, []string{"job_type"}),
		WorkerCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "task_queue_worker_count",
			Help: "Current number of active worker goroutines",
		}),
	}

	registerer.MustRegister(
		m.JobsCreated, m.JobsCompleted, m.JobsFailed, m.JobsRetried, m.DLQTotal,
		m.ProcessingTime, m.QueueDepth, m.WorkerCount,
	)
	return m
}

func (m *Metrics) JobCreated(jobType string, priority int) {
	m.JobsCreated.WithLabelValues(jobType, strconv.Itoa(priority)).Inc()
}

func (m *Metrics) JobCompleted(jobType string, durationMs int) {
	m.JobsCompleted.WithLabelValues(jobType).Inc()
	m.ProcessingTime.WithLabelValues(jobType).Observe(float64(durationMs) / 1000)
}

func (m *Metrics) JobFailed(jobType string) {
	m.JobsFailed.WithLabelValues(jobType).Inc()
	m.DLQTotal.Inc()
}

func (m *Metrics) JobRetried(jobType string) {
	m.JobsRetried.WithLabelValues(jobType).Inc()
}

func (m *Metrics) SetWorkerCount(count int) {
	m.WorkerCount.Set(float64(count))
}

func (m *Metrics) SetQueueDepth(jobType string, depth int64) {
	m.QueueDepth.WithLabelValues(jobType).Set(float64(depth))
}
