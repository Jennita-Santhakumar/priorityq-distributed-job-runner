CREATE TABLE dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    reason TEXT,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dlq_job_id ON dead_letter_queue(job_id);
