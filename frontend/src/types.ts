export type JobStatus =
  | "pending"
  | "processing"
  | "completed"
  | "failed"
  | "retried"
  | "cancelled";

export interface Job {
  id: string;
  type: string;
  status: JobStatus;
  payload?: unknown;
  result?: unknown;
  error_message?: string;
  priority: number;
  max_retries: number;
  retry_count: number;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  execution_time_ms?: number;
}

export interface Stats {
  pending_jobs: number;
  processing_jobs: number;
  completed_jobs: number;
  failed_jobs: number;
  dead_letter_count: number;
  avg_processing_time_ms: number;
  throughput_jobs_per_sec: number;
  queue_depth_by_type: Record<string, number>;
  worker_count: number;
  uptime_seconds: number;
}

export const JOB_TYPES = ["data_transform", "image_resize", "email_send"] as const;
export type JobType = (typeof JOB_TYPES)[number];

export const EXAMPLE_PAYLOADS: Record<JobType, string> = {
  data_transform: JSON.stringify(
    { numbers: [4, 8, 15, 16, 23, 42], operation: "sum" },
    null,
    2,
  ),
  image_resize: JSON.stringify(
    { image_base64: "<base64-encoded PNG/JPEG>", width: 128, height: 128 },
    null,
    2,
  ),
  email_send: JSON.stringify(
    { recipient: "demo@example.com", subject: "Hello", body: "This is a simulated email." },
    null,
    2,
  ),
};
