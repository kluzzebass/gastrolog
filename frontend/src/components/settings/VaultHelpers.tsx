import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useJob } from "../../api/hooks";
import { FormField, SelectInput } from "./FormField";
import { Button } from "./Buttons";
import type { Job } from "../../api/model/job";

// Phase 4 (gastrolog-42f9z): retention rules collapsed to just a policy
// trigger. The action / eject-route-ids fields are gone — the routing
// engine owns the "what happens to the records" decision via routes
// with Source = Retention trigger.
export interface RetentionRuleEdit {
  retentionPolicyId: string;
}

/** Returns true when the retention rule (if present) has required fields filled in. */
export function retentionRulesValid(rules: RetentionRuleEdit[]): boolean {
  return rules.every((r) => r.retentionPolicyId !== "");
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

export function RetentionRuleEditor({
  rules,
  onChange,
  retentionPolicies,
  dark,
}: Readonly<{
  rules: RetentionRuleEdit[];
  onChange: (rules: RetentionRuleEdit[]) => void;
  retentionPolicies: Array<{ id: string; name: string }>;
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
              : onChange([{ retentionPolicyId: "" }])
          }
        >
          {enabled ? "Remove" : "+ Add"}
        </Button>
      </div>
      {!enabled && (
        <span
          className={`text-[0.8em] italic ${c("text-text-muted", "text-light-text-muted")}`}
        >
          No retention rule
        </span>
      )}
      {enabled && (
        <FormField
          label="Policy"
          description="When this policy fires, the chunk's records are streamed through the routing engine and the chunk is destroyed. Configure routes with Source = Retention trigger to receive routed records."
          dark={dark}
        >
          <SelectInput
            value={rule.retentionPolicyId}
            onChange={(v) => onChange([{ retentionPolicyId: v }])}
            options={policyOptions}
            dark={dark}
          />
        </FormField>
      )}
    </div>
  );
}
