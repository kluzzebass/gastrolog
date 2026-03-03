import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useJob } from "../../api/hooks";
import { JobStatus } from "../../api/client";
import { FormField, SelectInput } from "./FormField";
import { Button } from "./Buttons";
import type { Job } from "../../api/gen/gastrolog/v1/job_pb";

export interface RetentionRuleEdit {
  retentionPolicyId: string;
  action: string;
  destinationId: string;
}

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
    if (job.status === JobStatus.COMPLETED) {
      handledRef.current = true;
      qc.invalidateQueries({ queryKey: ["vaults"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["indexes"] });
      qc.invalidateQueries({ queryKey: ["config"] });
      onComplete(job);
    } else if (job.status === JobStatus.FAILED) {
      handledRef.current = true;
      onFailed(job);
    }
  }, [job, onComplete, onFailed, qc]);

  if (!job) return null;

  const isRunning =
    job.status === JobStatus.RUNNING || job.status === JobStatus.PENDING;
  if (!isRunning) return null;

  const progress =
    job.chunksTotal > 0
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

export function RetentionRulesEditor({
  rules,
  onChange,
  retentionPolicies,
  vaults,
  currentVaultId,
  dark,
}: Readonly<{
  rules: RetentionRuleEdit[];
  onChange: (rules: RetentionRuleEdit[]) => void;
  retentionPolicies: Array<{ id: string; name: string }>;
  vaults: Array<{ id: string; name: string }>;
  currentVaultId: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const policyOptions = [
    { value: "", label: "(select policy)" },
    ...retentionPolicies.map((r) => ({ value: r.id, label: r.name || r.id })),
  ];
  const actionOptions = [
    { value: "expire", label: "expire" },
    { value: "migrate", label: "migrate" },
  ];
  const vaultOptions = [
    { value: "", label: "(select vault)" },
    ...vaults
      .filter((s) => s.id !== currentVaultId)
      .map((s) => ({ value: s.id, label: s.name || s.id })),
  ];

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Retention Rules
        </span>
        <Button
          variant="ghost"
          dark={dark}
          onClick={() =>
            onChange([
              ...rules,
              { retentionPolicyId: "", action: "expire", destinationId: "" },
            ])
          }
        >
          + Add
        </Button>
      </div>
      {rules.length === 0 && (
        <span
          className={`text-[0.8em] italic ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          No retention rules
        </span>
      )}
      {rules.map((rule, idx) => (
        <div key={idx} className="flex items-end gap-2">
          <div className="flex-1">
            <FormField label="Policy" dark={dark}>
              <SelectInput
                value={rule.retentionPolicyId}
                onChange={(v) => {
                  const next = rules.map((r, i) =>
                    i === idx ? { ...r, retentionPolicyId: v } : r,
                  );
                  onChange(next);
                }}
                options={policyOptions}
                dark={dark}
              />
            </FormField>
          </div>
          <div className="w-28">
            <FormField label="Action" dark={dark}>
              <SelectInput
                value={rule.action}
                onChange={(v) => {
                  const next = rules.map((r, i) =>
                    i === idx
                      ? { ...r, action: v, destinationId: v === "expire" ? "" : r.destinationId }
                      : r,
                  );
                  onChange(next);
                }}
                options={actionOptions}
                dark={dark}
              />
            </FormField>
          </div>
          {rule.action === "migrate" && (
            <div className="flex-1">
              <FormField label="Destination" dark={dark}>
                <SelectInput
                  value={rule.destinationId}
                  onChange={(v) => {
                    const next = rules.map((r, i) =>
                      i === idx ? { ...r, destinationId: v } : r,
                    );
                    onChange(next);
                  }}
                  options={vaultOptions}
                  dark={dark}
                />
              </FormField>
            </div>
          )}
          <Button variant="ghost"
            onClick={() => onChange(rules.filter((_, i) => i !== idx))}
            dark={dark}
          >
            Remove
          </Button>
        </div>
      ))}
    </div>
  );
}
