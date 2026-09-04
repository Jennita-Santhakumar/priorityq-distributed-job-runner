CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    result JSONB,
    error_message TEXT,
    priority INT NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 10),
    max_retries INT NOT NULL DEFAULT 3 CHECK (max_retries BETWEEN 0 AND 10),
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    scheduled_at TIMESTAMPTZ,
    worker_id VARCHAR(255),
    execution_time_ms INT,

    -- 'cancelled' added: the PRD's DELETE /api/v1/jobs/{id} endpoint cancels
    -- a job, but the PRD's own CHECK constraint has nowhere valid to record
    -- that outcome. Using 'failed' instead would corrupt failure-rate
    -- metrics and dead-letter-queue semantics.
    CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'retried', 'cancelled'))
);

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_type ON jobs(type);
CREATE INDEX idx_jobs_priority ON jobs(priority DESC);
CREATE INDEX idx_jobs_scheduled_at ON jobs(scheduled_at) WHERE status = 'pending';
CREATE INDEX idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX idx_jobs_worker_id ON jobs(worker_id);

-- Supports GetStats' queue_depth_by_type query, which groups active jobs by type.
CREATE INDEX idx_jobs_status_type ON jobs(status, type) WHERE status IN ('pending', 'processing');

-- Supports the stale-job reclaim background pass (processing jobs whose
-- updated_at is older than the reclaim threshold).
CREATE INDEX idx_jobs_processing_updated_at ON jobs(updated_at) WHERE status = 'processing';

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_set_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
