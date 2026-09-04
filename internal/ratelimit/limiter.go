// Package ratelimit provides a process-wide rate limiter for job submission.
//
// Scope note: the PRD's rate-limit spec ("100 jobs/min") is written against
// a per-user model, but this project has no auth/multi-tenancy system in
// scope (the PRD never designs one beyond the rate-limit mention). This
// limiter is therefore a single global token bucket shared by all
// submitters, not per-user/per-IP. Swapping to a per-key limiter (keyed by
// IP or an API key, with a cleanup goroutine for stale keys) is a one-file
// change if auth is ever added — this is called out explicitly in the
// README's Design Decisions.
package ratelimit

import (
	"net/http"

	"golang.org/x/time/rate"
)

func Middleware(requestsPerMinute, burst int) func(http.Handler) http.Handler {
	limiter := rate.NewLimiter(rate.Limit(float64(requestsPerMinute)/60.0), burst)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.Allow() {
				w.Header().Set("Retry-After", "60")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate limit exceeded","code":"rate_limited"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
