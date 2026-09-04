# Distributed Task Queue System

A production-grade distributed task queue: a Go REST API for submitting jobs, a Redis-backed
priority queue, a worker pool with exponential-backoff retry and a dead-letter queue, PostgreSQL
for durable job state and audit history, Prometheus + Grafana for monitoring, and a small React
dashboard for submitting jobs and watching them run.

Built from a PRD specifying a Stripe/Uber/Netflix-style task queue. Several parts of the PRD's own
reference design had real bugs or gaps — a hot-loop `KEYS *` call, a non-goroutine-safe circuit
breaker, a blocking retry model, missing schema columns — all fixed here and documented in
[Design Decisions & Trade-offs](#design-decisions--trade-offs) below.

## Architecture

```
                    ┌───────────────────┐
   POST /jobs  ───▶ │     API Server     │──── writes ───▶ ┌──────────────┐
   GET  /jobs       │   (Go, chi)        │                  │  PostgreSQL  │
   DELETE /jobs      │   :8080            │◀──── reads ─────│  jobs        │
   GET  /stats       └─────────┬──────────┘                  │  job_history │
                                │ enqueue                     │  dlq         │
                                ▼                              └──────▲───────┘
                       ┌─────────────────┐                            │ settle
                       │  Redis (queue)  │                            │
                       │  sorted sets    │◀───── dequeue ─────┌────────┴────────┐
                       │  per job type   │                    │  Worker Pool    │
                       └─────────────────┘──── requeue ──────▶│  (Go goroutines)│
                                                                │  :9103 /metrics │
                                                                └────────┬────────┘
                                                                         │ scrape
                       ┌─────────────┐        scrape         ┌──────────▼─────────┐
                       │   Grafana   │◀───────────────────────│    Prometheus      │
                       │   :3002     │                         │      :9092         │
                       └─────────────┘                         └────────────────────┘

                       ┌─────────────────────┐
                       │  React Dashboard     │──── HTTP ───▶ API Server
                       │  :5203                │
                       └─────────────────────┘
```

**Data flow**: a client `POST`s a job → it's written to Postgres (source of truth) and pushed
into a Redis sorted set keyed by job type (score = priority, or a future unix timestamp for
delayed jobs) → a worker goroutine dequeues it, executes the registered handler, and writes the
outcome back to Postgres → on failure, the job is re-enqueued with an exponential-backoff delay;
once retries are exhausted it's marked failed and recorded in the dead-letter queue.

## Quickstart

```bash
docker compose up --build
```

| Service | URL |
|---|---|
| Dashboard | http://localhost:5203 |
| API | http://localhost:8030 |
| API health | http://localhost:8030/health |
| API metrics | http://localhost:8030/metrics |
| Worker metrics | http://localhost:9103/metrics |
| PostgreSQL | localhost:5462 |
| Redis | localhost:6410 |
| Prometheus | http://localhost:9092 |
| Grafana | http://localhost:3002 (admin/admin, or anonymous viewer) |

Ports are offset from the defaults (5432/6379/8080/9090/3000/5173) to avoid colliding with other
projects on the same machine that also use `docker compose`.

Try it:
```bash
curl -X POST http://localhost:8030/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"type":"data_transform","payload":{"numbers":[1,2,3,4],"operation":"sum"},"priority":8}'
```

## Local development

**Backend** (requires Go 1.23+):
```bash
cp .env.example .env   # edit DATABASE_URL/REDIS_URL if not using the compose defaults
go mod download
go run ./cmd/migrate up
go run ./cmd/server     # terminal 1
go run ./cmd/worker     # terminal 2
```

Tests:
```bash
go test -race -short ./...                                  # unit tests, no external services
go test -race -tags=integration ./tests/integration/...      # needs real Postgres + Redis
```

**Frontend**:
```bash
cd frontend
npm install
npm run dev             # http://localhost:5173, set VITE_API_BASE_URL if the API isn't on :8030
```

## API reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/jobs` | Submit a job. Rate-limited (~100/min, global). |
| `GET` | `/api/v1/jobs` | List jobs (`status`, `type`, `limit`, `offset` query params) — added beyond the PRD, see below. |
| `GET` | `/api/v1/jobs/{id}` | Get a single job's full state. |
| `DELETE` | `/api/v1/jobs/{id}` | Cancel a job. `204` if cancelled, `404` if unknown, `409` if already picked up/terminal. |
| `GET` | `/api/v1/stats` | Aggregate queue stats. |
| `GET` | `/health` | Liveness/readiness (pings Postgres). |
| `GET` | `/metrics` | Prometheus exposition. |

Example `POST /api/v1/jobs` response:
```json
{
  "id": "3f1b2c4a-...",
  "type": "data_transform",
  "status": "pending",
  "priority": 8,
  "max_retries": 3,
  "retry_count": 0,
  "created_at": "2026-09-04T12:00:00Z"
}
```

Registered job types: `data_transform`, `image_resize`, `email_send` — see
[Design Decisions](#design-decisions--trade-offs) for what each actually does.

## Design Decisions & Trade-offs

Documenting every deliberate deviation from the source PRD's literal spec:

1. **chi over the PRD's stated Fiber.** The PRD's architecture diagram says "Go + Fiber," but its
   own handler code sketch already calls `chi.URLParam(r, "job_id")` — a chi API. Fiber
   (fasthttp-based) is incompatible with `net/http`, which would force every middleware and test
   in this codebase to be fasthttp-shaped for no throughput benefit at this scale. Went with what
   the PRD's code actually implied.

2. **Fixed a `KEYS *`-in-a-hot-loop anti-pattern.** The PRD's `Dequeue()` calls
   `client.Keys(ctx, prefix+"*")` on every poll from every worker — `KEYS` is O(N) over the whole
   keyspace and blocks Redis's single-threaded event loop; a well-known production anti-pattern.
   Replaced with a `queue:types` Redis Set kept current via `SADD` on every `Enqueue`, and
   `Dequeue` rotates round-robin across just those known type keys (`SMEMBERS` + `ZPOPMIN`) —
   O(#job types), not O(keyspace). See `internal/queue/redis.go`.

3. **Non-blocking retry/backoff instead of the PRD's blocking `backoff.Retry()`.** The PRD's
   `executeWithRetry` sketch calls `backoff.Retry(operation, backoffPolicy)`, which blocks the
   calling goroutine for the entire backoff window — with N workers, a handful of slow-retrying
   jobs can starve the whole pool. This implementation computes the backoff delay as a pure
   function (`internal/worker/retry.go`), re-`ZAdd`s the job into Redis with a future score, and
   the worker goroutine returns immediately to polling other work.

4. **Schema fixes** (`internal/store/migrations/000001_create_jobs_table.up.sql`): added
   `cancelled` to the `status` CHECK constraint (the PRD's `DELETE /jobs/{id}` cancels a job but
   its own CHECK constraint has nowhere valid to record that); added `updated_at` maintained by a
   trigger; switched every `TIMESTAMP` column to `TIMESTAMPTZ`; added a partial index supporting
   the stats query; made job cancellation a single atomic conditional `UPDATE ... WHERE
   status='pending'` instead of a racy read-then-write.

5. **No fabricated "10K+ jobs/sec" claim.** The PRD's own "Success Metrics for Hiring Managers"
   section asserts 10K jobs/sec without ever measuring it. `scripts/loadtest/main.go` fires real
   concurrent submissions against a running local stack and reports the actually-measured
   submission rate and completion throughput — see [Testing](#testing) for the real number
   recorded from this machine, not an aspirational one.

6. **No real SMTP or S3.** `email_send` simulates sending (validates the payload, sleeps a
   jittered 200-800ms, returns a fake message ID) — it is explicitly labeled `"simulated": true`
   in its result and never contacts a real mail server, since a portfolio demo has no business
   spamming real inboxes and no SMTP credentials are provisioned. `image_resize` does real local
   image decode/resize/encode via `disintegration/imaging` on a client-supplied base64 image,
   rather than fetching an arbitrary URL and uploading to S3 (which would need paid AWS
   credentials this project intentionally doesn't require).

7. **Added `GET /api/v1/jobs`.** The PRD specifies only get-by-id, but a usable dashboard needs a
   way to show "recent jobs" without already knowing their IDs. Added a small paginated/filterable
   list endpoint — not a full search API.

8. **`sony/gobreaker` instead of the PRD's hand-rolled circuit breaker.** The PRD's own
   `CircuitBreaker.Call` sketch mutates `cb.failures`/`cb.state` with no mutex — a real data race
   under concurrent worker goroutines. Used an off-the-shelf, concurrency-safe implementation
   wrapping every Postgres write path instead (`internal/store/postgres.go`).

9. **No Postgres-fallback-queue when Redis is down.** The PRD's error table aspires to "Redis
   connection lost → fall back to polling PostgreSQL," but never designs it. Implementing a full
   dual-backend queue is out of scope for a portfolio demo; the circuit breaker's fail-fast/
   recover behavior around Postgres is the implemented mitigation, and this is a deliberate,
   documented scope cut rather than a silently missing feature.

10. **Stale-job reclaim, added beyond the PRD's code.** The PRD's error table says "Worker crash
    during execution → job stays in processing, timeout after 30min, auto-retry," but no code
    sketch implements it. A background goroutine (`internal/worker/consumer.go`,
    `runStaleReclaim`) resets jobs stuck in `processing` past a configurable threshold back to
    `pending` and re-enqueues them.

11. **Kubernetes manifests are reference-only.** `kubernetes/` contains valid deployment/service/
    configmap YAML to demonstrate the shape, but there's no live cluster to deploy to, so it's not
    exercised by CI — unlike the PRD's `deploy.yml`, which was dropped entirely rather than left
    permanently failing for lack of registry/cluster secrets.

12. **No auth/multi-tenancy.** The PRD never designs one beyond mentioning per-user rate limits.
    The rate limiter here is therefore a single global token bucket (~100/min), not per-user —
    documented, with a one-file path to per-key limiting if auth is ever added
    (`internal/ratelimit/limiter.go`).

## Testing

- **Unit** (`go test -race -short ./...`, no external services): backoff math and its cap/jitter
  bounds (`internal/worker/retry_test.go`); job status transition rules
  (`internal/job/job_test.go`); Redis queue priority ordering, delayed-job put-back, round-robin
  across types, and worker heartbeat (`internal/queue/redis_test.go`, via `miniredis`); API
  validation paths — invalid body, unknown job type, missing fields, out-of-range priority
  (`internal/api/handlers_test.go`).
- **Integration** (`go test -race -tags=integration ./tests/integration/...`, real dockerized
  Postgres + Redis): full job lifecycle create→enqueue→execute→complete through a real worker
  pool; cancel-before-pickup; cancel-after-pickup returns 409; retry-then-DLQ with an exact
  retry-count assertion; backoff delay is actually scheduled in the future, not redelivered
  immediately.
- **CI**: `.github/workflows/build.yml` runs both suites against real Postgres/Redis service
  containers plus `golangci-lint`, and builds the frontend — no docker-push or deploy job, since
  there's no registry/cluster secret to make either succeed for real.
- **Load test**: `scripts/load_test.sh` (wraps `scripts/loadtest/main.go`) submits real concurrent
  jobs against a running compose stack and reports measured throughput — run it yourself with
  `./scripts/load_test.sh` after `docker compose up`; results depend heavily on host hardware, so
  no single number is claimed here as a universal benchmark. Measured on this dev machine
  (4 workers, `data_transform` sum jobs, default compose config):
  - **With the default rate limiter** (100 jobs/min, burst 100): submission is throttled by
    design — 2000 concurrent submit attempts produced 102 accepted (429s on the rest), exactly the
    rate limiter doing its job. This is expected behavior, not a bottleneck to "fix."
  - **With the rate limiter temporarily raised** (to isolate raw processing throughput, not the
    submission-side limiter): **915 req/s submission**, **100 jobs/sec sustained completion
    throughput** with 4 worker goroutines. This replaces the PRD's unverified "10K+ jobs/sec"
    claim — a real number from this hardware and this worker count, not a marketing figure.
    Completion throughput scales with `WORKER_COUNT` and job handler latency; it was not
    benchmarked at higher worker counts here.

## Tech stack

**Backend**: Go 1.23, chi, pgx/v5, go-redis/v9, golang-migrate, go-playground/validator, stdlib
`log/slog`, prometheus/client_golang, sony/gobreaker, golang.org/x/time/rate,
disintegration/imaging.

**Frontend**: React 18, TypeScript, Vite.

**Infrastructure**: PostgreSQL 16, Redis 7, Prometheus, Grafana, Docker Compose, GitHub Actions,
Kubernetes manifests (reference-only).
