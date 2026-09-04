import { useState } from "react";
import { SubmitJobForm } from "./components/SubmitJobForm";
import { JobList } from "./components/JobList";
import { StatsPanel } from "./components/StatsPanel";
import "./App.css";

export default function App() {
  const [trackedIds, setTrackedIds] = useState<string[]>([]);

  return (
    <div className="app">
      <header>
        <h1>Distributed Task Queue Dashboard</h1>
      </header>
      <main className="grid">
        <SubmitJobForm onSubmitted={(id) => setTrackedIds((prev) => [...prev, id])} />
        <JobList trackedIds={trackedIds} />
        <StatsPanel />
      </main>
    </div>
  );
}
