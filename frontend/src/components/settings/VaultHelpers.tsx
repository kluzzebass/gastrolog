import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useJob } from "../../api/hooks";
import type { Job } from "../../api/model/job";

export function JobProgress({
  jobId,
  label,
  dark,
  onComplete,
  onFailed,
}: Readonly<{
  jobId: string;
  label: string;
  dark: boolean;
  onComplete: (job: Job) => void;
  onFailed: (job: Job) => void;
}>) {
  const c = useThemeClass(dark);
  const { data: job } = useJob(jobId);
  const qc = useQueryClient();
  const handledRef = useRef(false);

  useEffect(() => {
    if (!job || handledRef.current) return;
    if (job.isCompleted) {
      handledRef.current = true;
      qc.invalidateQueries({ queryKey: ["vaults"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["indexes"] });
      qc.invalidateQueries({ queryKey: ["system"] });
      onComplete(job);
    } else if (job.isFailed) {
      handledRef.current = true;
      onFailed(job);
    }
  }, [job, onComplete, onFailed, qc]);

  if (!job) return null;

  if (!job.isActive) return null;

  const progress =
    job.chunksTotal > 0n
      ? `${job.chunksDone}/${job.chunksTotal} chunks`
      : "starting...";

  return (
    <div
      className={`flex items-center gap-2 px-3 py-1.5 text-[0.8em] rounded ${c(
        "bg-ink-hover text-text-muted",
        "bg-light-hover text-light-text-muted",
      )}`}
    >
      <span className="animate-spin inline-block w-3 h-3 border border-current border-t-transparent rounded-full" />
      <span>
        {label} {progress}
        {job.recordsDone > 0 && ` (${job.recordsDone} records)`}
      </span>
    </div>
  );
}
