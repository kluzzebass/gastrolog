import { useState, useEffect } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useWatchJobs } from "../../api/hooks";
import { toastError } from "../Toast";
import { JobStatus, JobKind } from "../../api/gen/gastrolog/v1/job_pb";
import type { Job } from "../../api/gen/gastrolog/v1/job_pb";
import { ExpandableCard } from "../settings/ExpandableCard";
import { HelpButton } from "../HelpButton";

/** Format a Date as `YYYY-MM-DD HH:MM:SS` (24-hour, local time). */
function formatTimestamp(date: Date): string {
  const y = date.getFullYear();
  const mo = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  const h = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  const s = String(date.getSeconds()).padStart(2, "0");
  return `${y}-${mo}-${d} ${h}:${mi}:${s}`;
}

/** Format elapsed time since a past date, e.g. "3m 12s ago", "1h 4m ago". */
function elapsed(date: Date): string {
  const diff = Date.now() - date.getTime();
  if (diff < 0) return formatTimestamp(date);

  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  const remSecs = secs % 60;
  if (mins < 60) return `${mins}m ${remSecs}s ago`;
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  if (hours < 24) return `${hours}h ${remMins}m ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h ago`;
}

/** Format a countdown to a future date, e.g. "in 42s", "in 3m 12s". */
function countdown(date: Date): string {
  const diff = date.getTime() - Date.now();
  if (diff <= 0) return "now";

  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `in ${secs}s`;
  const mins = Math.floor(secs / 60);
  const remSecs = secs % 60;
  if (mins < 60) return `in ${mins}m ${remSecs}s`;
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  if (hours < 24) return `in ${hours}h ${remMins}m`;
  const days = Math.floor(hours / 24);
  return `in ${days}d ${hours % 24}h`;
}

/** Ticks every second while mounted so countdowns/elapsed times stay current. */
function useTick() {
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, []);
}

export function JobsPanel({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { jobs, connected, reconnecting } = useWatchJobs({ onError: toastError });
  const [expanded, setExpanded] = useState<string | null>(null);
  useTick();

  const tasks = jobs.filter((j) => j.kind === JobKind.TASK);
  const scheduled = jobs.filter((j) => j.kind === JobKind.SCHEDULED);

  if (!connected && !reconnecting && jobs.length === 0) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Connecting...
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="flex items-center gap-2">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Jobs
        </h2>
        <HelpButton topicId="inspector-jobs" />
      </div>
      {reconnecting && (
        <div className="flex items-center gap-2">
          <span className="px-1.5 py-0.5 text-[0.75em] rounded bg-severity-warn/15 text-severity-warn">
            reconnecting
          </span>
        </div>
      )}

      {/* One-time tasks */}
      {tasks.length > 0 && (
        <section>
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-3 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Tasks
          </div>
          <div className="flex flex-col gap-2">
            {tasks.map((job) => (
              <ExpandableCard
                key={job.id}
                id={job.description || job.name || job.id}
                dark={dark}
                expanded={expanded === job.id}
                onToggle={() =>
                  setExpanded(expanded === job.id ? null : job.id)
                }
                status={<StatusBadge status={job.status} dark={dark} />}
                headerRight={<TaskProgress job={job} dark={dark} />}
              >
                <TaskDetail job={job} dark={dark} />
              </ExpandableCard>
            ))}
          </div>
        </section>
      )}

      {/* Scheduled jobs */}
      {scheduled.length > 0 && (
        <section>
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-3 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Scheduled
          </div>
          <div
            className={`border rounded-lg overflow-hidden ${c(
              "border-ink-border-subtle bg-ink-surface",
              "border-light-border-subtle bg-light-surface",
            )}`}
          >
            {/* Column headers */}
            <div
              className={`grid grid-cols-[1fr_8rem_9rem_9rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
                "text-text-ghost border-ink-border-subtle",
                "text-light-text-ghost border-light-border-subtle",
              )}`}
            >
              <span>Description</span>
              <span>Schedule</span>
              <span>Last run</span>
              <span>Next run</span>
            </div>

            {scheduled.map((job) => (
              <div
                key={job.id}
                className={`grid grid-cols-[1fr_8rem_9rem_9rem] gap-3 px-4 py-2 text-[0.85em] border-b last:border-b-0 ${c(
                  "border-ink-border-subtle",
                  "border-light-border-subtle",
                )}`}
              >
                <span
                  className={`font-mono truncate ${c("text-text-bright", "text-light-text-bright")}`}
                  title={job.name}
                >
                  {job.description || job.name || job.id}
                </span>
                <span
                  className={`font-mono text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}
                >
                  {job.schedule}
                </span>
                <span
                  className={`font-mono text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}
                  title={job.lastRun ? formatTimestamp(job.lastRun.toDate()) : ""}
                >
                  {job.lastRun ? elapsed(job.lastRun.toDate()) : "\u2014"}
                </span>
                <span
                  className={`font-mono text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}
                  title={job.nextRun ? formatTimestamp(job.nextRun.toDate()) : ""}
                >
                  {job.nextRun ? countdown(job.nextRun.toDate()) : "\u2014"}
                </span>
              </div>
            ))}
          </div>
        </section>
      )}

      {tasks.length === 0 && scheduled.length === 0 && (
        <div
          className={`flex items-center justify-center h-full text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          No active or scheduled jobs.
        </div>
      )}
    </div>
  );
}

function StatusBadge({
  status,
  dark,
}: Readonly<{
  status: JobStatus;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);

  switch (status) {
    case JobStatus.PENDING:
      return (
        <span
          className={`px-1.5 py-0.5 text-[0.75em] rounded ${c("bg-ink-hover text-text-ghost", "bg-light-hover text-light-text-ghost")}`}
        >
          pending
        </span>
      );
    case JobStatus.RUNNING:
      return (
        <span className="px-1.5 py-0.5 text-[0.75em] rounded bg-severity-info/15 text-severity-info">
          running
        </span>
      );
    case JobStatus.COMPLETED:
      return (
        <span className="px-1.5 py-0.5 text-[0.75em] rounded bg-copper/15 text-copper">
          completed
        </span>
      );
    case JobStatus.FAILED:
      return (
        <span className="px-1.5 py-0.5 text-[0.75em] rounded bg-severity-error/15 text-severity-error">
          failed
        </span>
      );
    default:
      return null;
  }
}

function TaskProgress({ job, dark }: Readonly<{ job: Job; dark: boolean }>) {
  const c = useThemeClass(dark);

  if (
    job.status !== JobStatus.RUNNING &&
    job.status !== JobStatus.COMPLETED &&
    job.status !== JobStatus.FAILED
  ) {
    return null;
  }

  const chunksTotal = Number(job.chunksTotal);
  const chunksDone = Number(job.chunksDone);
  const recordsDone = Number(job.recordsDone);

  if (chunksTotal === 0 && recordsDone === 0) return null;

  return (
    <span
      className={`text-[0.8em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      {chunksTotal > 0 && (
        <>
          {chunksDone}/{chunksTotal} chunks
        </>
      )}
      {recordsDone > 0 && (
        <>
          {chunksTotal > 0 && " \u00B7 "}
          {recordsDone.toLocaleString()} records
        </>
      )}
    </span>
  );
}

function TaskDetail({ job, dark }: Readonly<{ job: Job; dark: boolean }>) {
  const c = useThemeClass(dark);

  const stats: { label: string; value: string; isError?: boolean }[] = [];

  if (job.startedAt) {
    stats.push({
      label: "Started",
      value: formatTimestamp(job.startedAt.toDate()),
    });
  }
  if (job.completedAt) {
    stats.push({
      label: "Completed",
      value: formatTimestamp(job.completedAt.toDate()),
    });
  }
  if (job.error) {
    stats.push({ label: "Error", value: job.error, isError: true });
  }

  return (
    <div className={c("bg-ink-raised", "bg-light-bg")}>
      {stats.length > 0 && (
        <div className="flex flex-col gap-1.5">
          {stats.map((stat) => (
            <div
              key={stat.label}
              className="flex items-start gap-3 text-[0.85em]"
            >
              <span
                className={`w-24 shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}
              >
                {stat.label}
              </span>
              <span
                className={`font-mono ${
                  stat.isError
                    ? "text-severity-error"
                    : c("text-text-bright", "text-light-text-bright")
                }`}
              >
                {stat.value}
              </span>
            </div>
          ))}
        </div>
      )}

      {job.errorDetails.length > 0 && (
        <div className="mt-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Details
          </div>
          <div
            className={`text-[0.8em] font-mono space-y-1 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {job.errorDetails.map((detail, i) => (
              <div key={`err-${i}-${detail.slice(0, 50)}`}>{detail}</div>
            ))}
          </div>
        </div>
      )}

      {stats.length === 0 && job.errorDetails.length === 0 && (
        <div
          className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          No details available.
        </div>
      )}
    </div>
  );
}
