import { useState, useEffect } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import type { Job } from "../../api/model/job";
import {
  COUNTDOWN_MAX_CHARS,
  ELAPSED_MAX_CHARS,
  protoToInstant,
  formatTimestamp,
  elapsed,
  countdown,
} from "../../utils/temporal";
import { Badge } from "../Badge";
import { LiveValue } from "../LiveValue";
import { LIVE_TEXT } from "../liveValueStyle";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";

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
        className={`grid grid-cols-[minmax(0,1fr)_8rem_9rem_9rem] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
          "text-text-muted border-ink-border-subtle",
          "text-light-text-muted border-light-border-subtle",
        )}`}
      >
        <span>Job</span>
        <span>Schedule</span>
        <span className="text-right">Last run</span>
        <span className="text-right">Next run</span>
      </div>

      {jobs.map((job) => (
        <div
          key={job.id}
          className={`grid grid-cols-[minmax(0,1fr)_8rem_9rem_9rem] gap-3 px-4 py-2 text-[0.85em] border-b last:border-b-0 ${c(
            "border-ink-border-subtle",
            "border-light-border-subtle",
          )}`}
        >
          <span
            className={`flex items-center gap-2 min-w-0 ${c("text-text-bright", "text-light-text-bright")}`}
          >
            <span className="font-mono truncate" title={job.scheduleLabel}>
              {job.scheduleLabel}
            </span>
            {showNodeBadge && <NodeBadge nodeId={job.nodeId} dark={dark} />}
          </span>
          <span
            className={`font-mono text-[0.9em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
            title={job.displaySchedule ? `Schedule: ${job.displaySchedule}` : undefined}
          >
            {job.displaySchedule}
          </span>
          <JobRunTimes job={job} now={now} dark={dark} />
        </div>
      ))}
    </div>
  );
}

/**
 * The last-run and next-run cells of a scheduled job. They re-render every
 * second, so each reserves the widest string its formatter emits and keeps
 * the unit text pinned to the right edge while the digits tick.
 */
export function JobRunTimes({ job, now, dark }: Readonly<{ job: Job; now: number; dark: boolean }>) {
  return (
    <>
      <LiveValue
        dark={dark}
        tone="muted"
        reserve={ELAPSED_MAX_CHARS}
        className="text-[0.9em]"
        title={job.lastRun ? formatTimestamp(protoToInstant(job.lastRun)) : ""}
      >
        {job.lastRun ? elapsed(protoToInstant(job.lastRun), now) : "—"}
      </LiveValue>
      <LiveValue
        dark={dark}
        tone="muted"
        reserve={COUNTDOWN_MAX_CHARS}
        className="text-[0.9em]"
        title={job.nextRun ? formatTimestamp(protoToInstant(job.nextRun)) : ""}
      >
        {job.nextRun ? countdown(protoToInstant(job.nextRun), now) : "—"}
      </LiveValue>
    </>
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
      className={`text-[0.8em] font-mono ${LIVE_TEXT} ${c("text-text-muted", "text-light-text-muted")}`}
    >
      {chunksTotal > 0 && <ChunkProgress done={chunksDone} total={chunksTotal} dark={dark} />}
      {recordsDone > 0 && (
        <>
          {chunksTotal > 0 && " · "}
          {recordsDone.toLocaleString()} records
        </>
      )}
    </span>
  );
}

/** "done/total chunks", with the done count reserving the width of the total it counts toward. */
export function ChunkProgress({ done, total, dark }: Readonly<{ done: number; total: number; dark: boolean }>) {
  return (
    <>
      <LiveValue dark={dark} tone="inherit" reserve={String(total).length}>
        {done}
      </LiveValue>
      /{total} chunks
    </>
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
