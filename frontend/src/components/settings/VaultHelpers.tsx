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
  ejectRouteIds: string[];
}

/** Returns true when the retention rule (if present) has required fields filled in. */
export function retentionRulesValid(rules: RetentionRuleEdit[]): boolean {
  return rules.every(
    (r) =>
      r.retentionPolicyId !== "" &&
      (r.action !== "eject" || r.ejectRouteIds.length > 0),
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
  routes,
  dark,
}: Readonly<{
  rules: RetentionRuleEdit[];
  onChange: (rules: RetentionRuleEdit[]) => void;
  retentionPolicies: Array<{ id: string; name: string }>;
  routes: Array<{ id: string; name: string; ejectOnly: boolean }>;
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
    { value: "eject", label: "eject" },
  ];

  // Only eject-only routes are eligible as eject targets.
  const ejectRoutes = routes
    .filter((r) => r.ejectOnly)
    .sort((a, b) => a.name.localeCompare(b.name));

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
              : onChange([{ retentionPolicyId: "", action: "expire", ejectRouteIds: [] }])
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
        <div className="flex flex-col gap-2">
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
                    setRule({
                      action: v,
                      ejectRouteIds: v === "eject" ? rule.ejectRouteIds : [],
                    })
                  }
                  options={actionOptions}
                  dark={dark}
                />
              </FormField>
            </div>
          </div>
          {rule.action === "eject" && (
            <EjectRoutesPicker
              selectedIds={rule.ejectRouteIds}
              onChange={(ids) => setRule({ ejectRouteIds: ids })}
              routes={ejectRoutes}
              dark={dark}
            />
          )}
        </div>
      )}
    </div>
  );
}

function EjectRoutesPicker({
  selectedIds,
  onChange,
  routes,
  dark,
}: Readonly<{
  selectedIds: string[];
  onChange: (ids: string[]) => void;
  routes: Array<{ id: string; name: string }>;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const usedIds = new Set(selectedIds);
  const available = routes.filter((r) => !usedIds.has(r.id));

  return (
    <FormField label="Eject Routes" dark={dark}>
      <div className="flex flex-col gap-1.5">
        {selectedIds.map((id) => {
          const route = routes.find((r) => r.id === id);
          return (
            <div key={id} className="flex items-center gap-2">
              <span
                className={`flex-1 text-[0.85em] px-2.5 py-1.5 border rounded ${c(
                  "bg-ink-surface border-ink-border text-text-bright",
                  "bg-light-surface border-light-border text-light-text-bright",
                )}`}
              >
                {route?.name || id}
              </span>
              <Button
                variant="ghost"
                onClick={() => onChange(selectedIds.filter((rid) => rid !== id))}
                dark={dark}
              >
                Remove
              </Button>
            </div>
          );
        })}
        {available.length > 0 && (
          <SelectInput
            value=""
            onChange={(v) => {
              if (v) onChange([...selectedIds, v]);
            }}
            options={[
              { value: "", label: "Add route\u2026" },
              ...available.map((r) => ({ value: r.id, label: r.name || r.id })),
            ]}
            dark={dark}
          />
        )}
        {selectedIds.length === 0 && available.length === 0 && (
          <p className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            No eject-only routes available. Create a route with "Eject Only" enabled first.
          </p>
        )}
      </div>
    </FormField>
  );
}
