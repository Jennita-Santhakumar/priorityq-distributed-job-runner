CREATE TABLE job_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    status VARCHAR(50) NOT NULL,
    message TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    worker_id VARCHAR(255)
);

CREATE INDEX idx_job_history_job_id ON job_history(job_id);
CREATE INDEX idx_job_history_recorded_at ON job_history(recorded_at DESC);
