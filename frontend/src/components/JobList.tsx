import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { Job } from "../types";
import { JobDetail } from "./JobDetail";

const TERMINAL_STATUSES = new Set(["completed", "failed", "cancelled"]);

interface Props {
  trackedIds: string[];
}

export function JobList({ trackedIds }: Props) {
  const [jobs, setJobs] = useState<Record<string, Job>>({});
  const [selectedId, setSelectedId] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function pollOnce() {
      for (const id of trackedIds) {
        const existing = jobs[id];
        if (existing && TERMINAL_STATUSES.has(existing.status)) continue;
        try {
          const job = await api.getJob(id);
          if (!cancelled) {
            setJobs((prev) => ({ ...prev, [id]: job }));
          }
        } catch {
          // job may not have propagated to the read path yet; retry next tick
        }
      }
    }

    pollOnce();
    const interval = setInterval(pollOnce, 2000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [trackedIds]);

  if (trackedIds.length === 0) {
    return (
      <div className="panel">
        <h2>Jobs</h2>
        <p>No jobs submitted this session yet.</p>
      </div>
    );
  }

  return (
    <div className="panel">
      <h2>Jobs</h2>
      <ul className="job-list">
        {trackedIds
          .slice()
          .reverse()
          .map((id) => {
            const job = jobs[id];
            return (
              <li
                key={id}
                className={selectedId === id ? "selected" : ""}
                onClick={() => setSelectedId(id)}
              >
                <span className={`badge badge-${job?.status ?? "pending"}`}>
                  {job?.status ?? "loading"}
                </span>
                <code>{id.slice(0, 8)}</code>
                <span>{job?.type}</span>
              </li>
            );
          })}
      </ul>
      {selectedId && jobs[selectedId] && (
        <JobDetail job={jobs[selectedId]} onCancelled={() => setSelectedId(selectedId)} />
      )}
    </div>
  );
}
