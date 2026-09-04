package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/KabileshRajaselvan/task-queue-system/internal/ratelimit"
)

type RouterConfig struct {
	RateLimitPerMin int
	RateLimitBurst  int
	CORSOrigins     []string
}

func NewRouter(h *Handler, cfg RouterConfig, logger *slog.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(RequestLogger(logger))

	r.Get("/health", h.Health)
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		r.With(ratelimit.Middleware(cfg.RateLimitPerMin, cfg.RateLimitBurst)).Post("/jobs", h.CreateJob)
		r.Get("/jobs", h.ListJobs)
		r.Get("/jobs/{job_id}", h.GetJob)
		r.Delete("/jobs/{job_id}", h.CancelJob)
		r.Get("/stats", h.GetStats)
	})

	return withCORS(r, cfg.CORSOrigins)
}

func withCORS(next http.Handler, allowedOrigins []string) http.Handler {
	if len(allowedOrigins) == 0 {
		return next
	}
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
