// Package queue implements the Redis-backed priority queue.
//
// Design note: the source PRD's own Dequeue() calls client.Keys(ctx,
// prefix+"*") on every single dequeue attempt — from every idle worker,
// every poll cycle. KEYS is O(N) over the entire keyspace and blocks
// Redis's single-threaded event loop; it is a well-known production
// anti-pattern the Redis docs themselves warn against. This implementation
// instead maintains a small "queue:types" Set of known job types (kept
// current via SADD on every Enqueue) and rotates across just those type
// keys on Dequeue — O(#job types), not O(keyspace).
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/KabileshRajaselvan/task-queue-system/internal/job"
)

const (
	queuePrefix = "queue:"
	typesSetKey = "queue:types"

	// scores 0-10 encode immediate-priority jobs (10-priority); anything at
	// or above this threshold is a future unix timestamp for a delayed job.
	// A unix timestamp in seconds is always far larger than 10, so this
	// threshold cleanly separates the two score spaces.
	delayedScoreThreshold = 1000

	workerHeartbeatKey = "queue:worker_heartbeat_count"
	workerHeartbeatTTL = 30 * time.Second
)

type RedisQueue struct {
	client   *redis.Client
	rrOffset uint64 // rotating offset for round-robin dequeue across job types
}

func NewRedisQueue(client *redis.Client) *RedisQueue {
	return &RedisQueue{client: client}
}

func typeKey(jobType string) string {
	return queuePrefix + jobType
}

// queueEntry is the minimal payload stored as a sorted-set member. Only the
// job ID and type are kept here — the authoritative job state lives in
// Postgres (already written by the time Enqueue is called), so the queue
// never risks going stale relative to the database.
type queueEntry struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

func priorityScore(priority int) float64 {
	return float64(10 - priority)
}

// Enqueue adds a job to its type's sorted set, using priority as the score
// for immediate jobs (lower score = higher priority, matching the PRD's own
// scheme) or the scheduled unix timestamp for delayed jobs. It also
// registers the job's type in the types set in the same round trip via a
// pipeline, so Dequeue never needs to scan the keyspace to discover types.
func (q *RedisQueue) Enqueue(ctx context.Context, j *job.Job) error {
	entry, err := json.Marshal(queueEntry{ID: j.ID, Type: j.Type})
	if err != nil {
		return fmt.Errorf("failed to marshal queue entry: %w", err)
	}

	score := priorityScore(j.Priority)
	if j.ScheduledAt != nil && j.ScheduledAt.After(time.Now()) {
		score = float64(j.ScheduledAt.Unix())
	}

	key := typeKey(j.Type)
	_, err = q.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: entry})
		pipe.SAdd(ctx, typesSetKey, j.Type)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to enqueue job %s: %w", j.ID, err)
	}
	return nil
}

// Dequeue returns the next ready job across all registered types, or
// (nil, nil) if nothing is ready right now. It round-robins its starting
// point across known types on each call so no single type can starve
// others when multiple types have ready work.
func (q *RedisQueue) Dequeue(ctx context.Context) (*job.Job, error) {
	types, err := q.client.SMembers(ctx, typesSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list queue types: %w", err)
	}
	if len(types) == 0 {
		return nil, nil
	}

	start := int(atomic.AddUint64(&q.rrOffset, 1)) % len(types)
	order := append(append([]string{}, types[start:]...), types[:start]...)

	for _, jobType := range order {
		key := typeKey(jobType)
		popped, err := q.client.ZPopMin(ctx, key, 1).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to pop from %s: %w", key, err)
		}
		if len(popped) == 0 {
			continue
		}

		z := popped[0]
		if z.Score >= delayedScoreThreshold && float64(time.Now().Unix()) < z.Score {
			// Not yet due — put it back and try the next type. Safe under
			// concurrent workers: worst case two workers both put the same
			// not-yet-due item back, which is harmless (no duplicate exec).
			if _, err := q.client.ZAdd(ctx, key, redis.Z{Score: z.Score, Member: z.Member}).Result(); err != nil {
				return nil, fmt.Errorf("failed to put back delayed job: %w", err)
			}
			continue
		}

		member, ok := z.Member.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected queue member type %T", z.Member)
		}
		var entry queueEntry
		if err := json.Unmarshal([]byte(member), &entry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal queue entry: %w", err)
		}
		return &job.Job{ID: entry.ID, Type: entry.Type}, nil
	}

	return nil, nil
}

// Remove deletes a job's entry from its type's sorted set, regardless of
// its current score (immediate or delayed). It reconstructs the exact
// member bytes Enqueue would have written, since ZREM matches on member
// value, not job ID alone.
func (q *RedisQueue) Remove(ctx context.Context, jobType, jobID string) error {
	entry, err := json.Marshal(queueEntry{ID: jobID, Type: jobType})
	if err != nil {
		return fmt.Errorf("failed to marshal queue entry: %w", err)
	}
	if err := q.client.ZRem(ctx, typeKey(jobType), entry).Err(); err != nil {
		return fmt.Errorf("failed to remove job %s from queue: %w", jobID, err)
	}
	return nil
}

func (q *RedisQueue) Depth(ctx context.Context, jobType string) (int64, error) {
	return q.client.ZCard(ctx, typeKey(jobType)).Result()
}

func (q *RedisQueue) PublishWorkerHeartbeat(ctx context.Context, count int) error {
	return q.client.Set(ctx, workerHeartbeatKey, count, workerHeartbeatTTL).Err()
}

func (q *RedisQueue) WorkerHeartbeatCount(ctx context.Context) (int, error) {
	count, err := q.client.Get(ctx, workerHeartbeatKey).Int()
	if err == redis.Nil {
		return 0, nil
	}
	return count, err
}

func (q *RedisQueue) DepthByType(ctx context.Context) (map[string]int64, error) {
	types, err := q.client.SMembers(ctx, typesSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list queue types: %w", err)
	}
	depths := make(map[string]int64, len(types))
	for _, t := range types {
		depth, err := q.Depth(ctx, t)
		if err != nil {
			return nil, err
		}
		depths[t] = depth
	}
	return depths, nil
}
