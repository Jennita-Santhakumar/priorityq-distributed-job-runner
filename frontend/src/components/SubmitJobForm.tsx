import { useState } from "react";
import { api } from "../api/client";
import { EXAMPLE_PAYLOADS, JOB_TYPES, type JobType } from "../types";

interface Props {
  onSubmitted: (jobId: string) => void;
}

export function SubmitJobForm({ onSubmitted }: Props) {
  const [type, setType] = useState<JobType>("data_transform");
  const [payloadText, setPayloadText] = useState(EXAMPLE_PAYLOADS.data_transform);
  const [priority, setPriority] = useState(5);
  const [scheduledAt, setScheduledAt] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [lastJobId, setLastJobId] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  function handleTypeChange(next: JobType) {
    setType(next);
    setPayloadText(EXAMPLE_PAYLOADS[next]);
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    let payload: unknown;
    try {
      payload = JSON.parse(payloadText);
    } catch {
      setError("Payload must be valid JSON");
      return;
    }

    setSubmitting(true);
    try {
      const job = await api.createJob({
        type,
        payload,
        priority,
        max_retries: 3,
        scheduled_at: scheduledAt ? new Date(scheduledAt).toISOString() : undefined,
      });
      setLastJobId(job.id);
      onSubmitted(job.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="panel">
      <h2>Submit Job</h2>

      <label>
        Job type
        <select value={type} onChange={(e) => handleTypeChange(e.target.value as JobType)}>
          {JOB_TYPES.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </label>

      <label>
        Payload (JSON)
        <textarea
          value={payloadText}
          onChange={(e) => setPayloadText(e.target.value)}
          rows={6}
        />
      </label>

      <label>
        Priority: {priority}
        <input
          type="range"
          min={0}
          max={10}
          value={priority}
          onChange={(e) => setPriority(Number(e.target.value))}
        />
      </label>

      <label>
        Scheduled at (optional)
        <input
          type="datetime-local"
          value={scheduledAt}
          onChange={(e) => setScheduledAt(e.target.value)}
        />
      </label>

      <button type="submit" disabled={submitting}>
        {submitting ? "Submitting..." : "Submit Job"}
      </button>

      {error && <p className="error">{error}</p>}
      {lastJobId && <p className="success">Submitted job: {lastJobId}</p>}
    </form>
  );
}
