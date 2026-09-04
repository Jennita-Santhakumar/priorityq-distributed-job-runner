import { useState } from "react";
import { api } from "../api/client";
import type { Job } from "../types";

interface Props {
  job: Job;
  onCancelled: () => void;
}

export function JobDetail({ job, onCancelled }: Props) {
  const [cancelling, setCancelling] = useState(false);
  const [cancelError, setCancelError] = useState<string | null>(null);

  async function handleCancel() {
    setCancelling(true);
    setCancelError(null);
    try {
      await api.cancelJob(job.id);
      onCancelled();
    } catch (err) {
      setCancelError(err instanceof Error ? err.message : String(err));
    } finally {
      setCancelling(false);
    }
  }

  return (
    <div className="job-detail">
      <dl>
        <dt>ID</dt>
        <dd>{job.id}</dd>
        <dt>Type</dt>
        <dd>{job.type}</dd>
        <dt>Status</dt>
        <dd>{job.status}</dd>
        <dt>Priority</dt>
        <dd>{job.priority}</dd>
        <dt>Retries</dt>
        <dd>
          {job.retry_count} / {job.max_retries}
        </dd>
        {job.execution_time_ms != null && (
          <>
            <dt>Execution time</dt>
            <dd>{job.execution_time_ms} ms</dd>
          </>
        )}
        {job.error_message && (
          <>
            <dt>Error</dt>
            <dd className="error">{job.error_message}</dd>
          </>
        )}
      </dl>

      {job.payload != null && (
        <>
          <h4>Payload</h4>
          <pre>{JSON.stringify(job.payload, null, 2)}</pre>
        </>
      )}
      {job.result != null && (
        <>
          <h4>Result</h4>
          <pre>{JSON.stringify(job.result, null, 2)}</pre>
        </>
      )}

      {job.status === "pending" && (
        <button onClick={handleCancel} disabled={cancelling}>
          {cancelling ? "Cancelling..." : "Cancel job"}
        </button>
      )}
      {cancelError && <p className="error">{cancelError}</p>}
    </div>
  );
}
