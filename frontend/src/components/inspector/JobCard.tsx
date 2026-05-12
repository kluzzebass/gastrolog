import { useState, useEffect } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import type { Job } from "../../api/model/job";
import { protoToInstant, formatTimestamp, elapsed, countdown } from "../../utils/temporal";
import { Badge } from "../Badge";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";

export { formatTimestamp, elapsed, countdown } from "../../utils/temporal";

/** Ticks every second, returning Date.now() so time-dependent expressions
 *  have a compiler-visible dependency that changes each tick. */
export function useTick(): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(() => Date.now()), 1000);
    return () => clearInterval(id);
  }, []);
  return now;
}

// ---- Components ----

interface JobCardProps {
  job: Job;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  showNodeBadge?: boolean;
}

export function JobCard({
  job,
  dark,
  expanded,
  onToggle,
  showNodeBadge = true,
}: Readonly<JobCardProps>) {
  return (
    <ExpandableCard
      id={job.displayLabel}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      status={
        <span className="flex items-center gap-1.5">
          <StatusBadge job={job} dark={dark} />
          {showNodeBadge && <NodeBadge nodeId={job.nodeId} dark={dark} />}
        </span>
      }
      headerRight={<TaskProgress job={job} dark={dark} />}
    >
      <TaskDetail job={job} dark={dark} />
    </ExpandableCard>
  );
}

interface ScheduledJobsTableProps {
  jobs: Job[];
  dark: boolean;
  showNodeBadge?: boolean;
}

export function ScheduledJobsTable({
  jobs,
  dark,
  showNodeBadge = true,
}: Readonly<ScheduledJobsTableProps>) {
  const c = useThemeClass(dark);
  const now = useTick();

  if (jobs.length === 0) return null;

  return (
    <div
      className={`border rounded-lg overflow-hidden ${c(
        "border-ink-border-subtle bg-ink-surface",
        "border-light-border-subtle bg-light-surface",
      )}`}
    >
      {/* Column headers */}
      <div
        className={`grid grid-cols-[1fr_8rem_9rem_9rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
          "text-text-muted border-ink-border-subtle",
          "text-light-text-muted border-light-border-subtle",
        )}`}
      >
        <span>Description</span>
        <span>Schedule</span>
        <span>Last run</span>
        <span>Next run</span>
      </div>

      {jobs.map((job) => (
        <div
          key={job.id}
          className={`grid grid-cols-[1fr_8rem_9rem_9rem] gap-3 px-4 py-2 text-[0.85em] border-b last:border-b-0 ${c(
            "border-ink-border-subtle",
            "border-light-border-subtle",
          )}`}
        >
          <span
            className={`flex items-center gap-2 min-w-0 ${c("text-text-bright", "text-light-text-bright")}`}
          >
            <span className="font-mono truncate" title={job.displayLabel}>
              {job.displayLabel}
            </span>
            {showNodeBadge && <NodeBadge nodeId={job.nodeId} dark={dark} />}
          </span>
          <span
            className={`font-mono text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {job.schedule}
          </span>
          <span
            className={`font-mono text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}
            title={job.lastRun ? formatTimestamp(protoToInstant(job.lastRun)) : ""}
          >
            {job.lastRun ? elapsed(protoToInstant(job.lastRun), now) : "—"}
          </span>
          <span
            className={`font-mono text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}
            title={job.nextRun ? formatTimestamp(protoToInstant(job.nextRun)) : ""}
          >
            {job.nextRun ? countdown(protoToInstant(job.nextRun), now) : "—"}
          </span>
        </div>
      ))}
    </div>
  );
}

function StatusBadge({ job, dark }: Readonly<{ job: Job; dark: boolean }>) {
  const label = job.statusLabel;
  if (!label) return null;
  return <Badge variant={job.statusVariant} dark={dark}>{label}</Badge>;
}

function TaskProgress({ job, dark }: Readonly<{ job: Job; dark: boolean }>) {
  const c = useThemeClass(dark);

  if (!job.hasProgressSurface) return null;

  const chunksTotal = Number(job.chunksTotal);
  const chunksDone = Number(job.chunksDone);
  const recordsDone = Number(job.recordsDone);

  if (chunksTotal === 0 && recordsDone === 0) return null;

  return (
    <span
      className={`text-[0.8em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
    >
      {chunksTotal > 0 && (
        <>
          {chunksDone}/{chunksTotal} chunks
        </>
      )}
      {recordsDone > 0 && (
        <>
          {chunksTotal > 0 && " · "}
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
      value: formatTimestamp(protoToInstant(job.startedAt)),
    });
  }
  if (job.completedAt) {
    stats.push({
      label: "Completed",
      value: formatTimestamp(protoToInstant(job.completedAt)),
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
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Details
          </div>
          <div
            className={`text-[0.8em] font-mono space-y-1 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {job.errorDetails.map((detail) => (
              <div key={detail}>{detail}</div>
            ))}
          </div>
        </div>
      )}

      {stats.length === 0 && job.errorDetails.length === 0 && (
        <div
          className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          No details available.
        </div>
      )}
    </div>
  );
}
