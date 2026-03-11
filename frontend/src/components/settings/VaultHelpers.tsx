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

/** Returns true when the retention rule (if present) has required fields filled in. */
export function retentionRulesValid(rules: RetentionRuleEdit[]): boolean {
  return rules.every(
    (r) =>
      r.retentionPolicyId !== "" &&
      (r.action !== "migrate" || r.destinationId !== ""),
  );
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

export function RetentionRuleEditor({
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
  const rule = rules[0] as RetentionRuleEdit | undefined;
  const enabled = !!rule;

  const policyOptions = [
    { value: "", label: "(select policy)" },
    ...retentionPolicies
      .map((r) => ({ value: r.id, label: r.name || r.id }))
      .sort((a, b) => a.label.localeCompare(b.label)),
  ];
  const actionOptions = [
    { value: "expire", label: "expire" },
    { value: "migrate", label: "migrate" },
  ];
  const vaultOptions = [
    { value: "", label: "(select vault)" },
    ...vaults
      .filter((s) => s.id !== currentVaultId)
      .map((s) => ({ value: s.id, label: s.name || s.id }))
      .sort((a, b) => a.label.localeCompare(b.label)),
  ];

  const setRule = (patch: Partial<RetentionRuleEdit>) => {
    if (!rule) return;
    onChange([{ ...rule, ...patch }]);
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Retention Rule
        </span>
        <Button
          variant="ghost"
          dark={dark}
          onClick={() =>
            enabled
              ? onChange([])
              : onChange([{ retentionPolicyId: "", action: "expire", destinationId: "" }])
          }
        >
          {enabled ? "Remove" : "+ Add"}
        </Button>
      </div>
      {!enabled && (
        <span
          className={`text-[0.8em] italic ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          No retention rule
        </span>
      )}
      {enabled && (
        <div className="flex items-end gap-2">
          <div className="flex-1">
            <FormField label="Policy" dark={dark}>
              <SelectInput
                value={rule.retentionPolicyId}
                onChange={(v) => setRule({ retentionPolicyId: v })}
                options={policyOptions}
                dark={dark}
              />
            </FormField>
          </div>
          <div className="w-28">
            <FormField label="Action" dark={dark}>
              <SelectInput
                value={rule.action}
                onChange={(v) =>
                  setRule({ action: v, destinationId: v === "expire" ? "" : rule.destinationId })
                }
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
                  onChange={(v) => setRule({ destinationId: v })}
                  options={vaultOptions}
                  dark={dark}
                />
              </FormField>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
