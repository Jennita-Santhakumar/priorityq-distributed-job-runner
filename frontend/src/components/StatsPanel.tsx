import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { Stats } from "../types";

export function StatsPanel() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      try {
        const s = await api.getStats();
        if (!cancelled) {
          setStats(s);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      }
    }
    poll();
    const interval = setInterval(poll, 3000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  if (error) return <div className="panel error">Failed to load stats: {error}</div>;
  if (!stats) return <div className="panel">Loading stats...</div>;

  const tiles: [string, number | string][] = [
    ["Pending", stats.pending_jobs],
    ["Processing", stats.processing_jobs],
    ["Completed", stats.completed_jobs],
    ["Failed", stats.failed_jobs],
    ["Dead Letter", stats.dead_letter_count],
    ["Workers", stats.worker_count],
    ["Avg time (ms)", Math.round(stats.avg_processing_time_ms)],
    ["Throughput/s", stats.throughput_jobs_per_sec.toFixed(2)],
  ];

  return (
    <div className="panel">
      <h2>Queue Stats</h2>
      <div className="stat-tiles">
        {tiles.map(([label, value]) => (
          <div key={label} className="stat-tile">
            <div className="stat-value">{value}</div>
            <div className="stat-label">{label}</div>
          </div>
        ))}
      </div>

      <h3>Queue Depth by Type</h3>
      {Object.keys(stats.queue_depth_by_type).length === 0 ? (
        <p>No queued jobs.</p>
      ) : (
        <ul className="depth-list">
          {Object.entries(stats.queue_depth_by_type).map(([type, depth]) => (
            <li key={type}>
              <span>{type}</span>
              <span>{depth}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
