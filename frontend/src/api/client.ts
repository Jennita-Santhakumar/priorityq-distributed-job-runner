import type { Job, Stats } from "../types";

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8030";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

export interface CreateJobRequest {
  type: string;
  payload: unknown;
  priority: number;
  max_retries: number;
  scheduled_at?: string;
}

export const api = {
  createJob: (req: CreateJobRequest) =>
    request<Job>("/api/v1/jobs", { method: "POST", body: JSON.stringify(req) }),
  getJob: (id: string) => request<Job>(`/api/v1/jobs/${id}`),
  cancelJob: (id: string) => request<void>(`/api/v1/jobs/${id}`, { method: "DELETE" }),
  getStats: () => request<Stats>("/api/v1/stats"),
};
