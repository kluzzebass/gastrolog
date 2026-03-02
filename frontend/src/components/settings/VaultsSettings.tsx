import { useState, useEffect, useRef } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import {
  useConfig,
  usePutVault,
  useDeleteVault,
  useSealVault,
  useReindexVault,
  useMigrateVault,
  useMergeVaults,
  useJob,
  useGenerateName,
} from "../../api/hooks";
import { JobStatus } from "../../api/client";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { VaultParamsForm } from "./VaultParamsForm";
import { PrimaryButton, GhostButton } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { NodeBadge } from "./NodeBadge";
import { NodeSelect } from "./NodeSelect";
import { useQueryClient } from "@tanstack/react-query";
import type { Job } from "../../api/gen/gastrolog/v1/job_pb";

interface RetentionRuleEdit {
  retentionPolicyId: string;
  action: string;
  destinationId: string;
}

function JobProgress({
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

function RetentionRulesEditor({
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
        <button
          type="button"
          onClick={() =>
            onChange([
              ...rules,
              { retentionPolicyId: "", action: "expire", destinationId: "" },
            ])
          }
          className={`text-[0.8em] transition-colors ${c(
            "text-copper hover:text-copper-light",
            "text-copper hover:text-copper-light",
          )}`}
        >
          + Add
        </button>
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
          <GhostButton
            onClick={() => onChange(rules.filter((_, i) => i !== idx))}
            dark={dark}
          >
            Remove
          </GhostButton>
        </div>
      ))}
    </div>
  );
}

export function VaultsSettings({ dark, expandTarget, onExpandTargetConsumed }: Readonly<{ dark: boolean; expandTarget?: string | null; onExpandTargetConsumed?: () => void }>) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putVault = usePutVault();
  const deleteVault = useDeleteVault();
  const seal = useSealVault();
  const reindex = useReindexVault();
  const migrate = useMigrateVault();
  const merge = useMergeVaults();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [typeConfirmed, setTypeConfirmed] = useState(false);
  const [migrateTarget, setMigrateTarget] = useState<
    Record<string, { name: string; type: string; dir: string }>
  >({});
  const [mergeTarget, setMergeTarget] = useState<Record<string, string>>({});
  // Track active jobs per vault: { vaultId: { jobId, label } }
  const [activeJobs, setActiveJobs] = useState<
    Record<string, { jobId: string; label: string }>
  >({});

  const generateName = useGenerateName();

  // New vault form state.
  const [newName, setNewName] = useState("");
  const [namePlaceholder, setNamePlaceholder] = useState("");
  const [newType, setNewType] = useState("memory");
  const [newPolicy, setNewPolicy] = useState("");
  const [newRetentionRules, setNewRetentionRules] = useState<RetentionRuleEdit[]>([]);
  const [newParams, setNewParams] = useState<Record<string, string>>({});
  const [newNodeId, setNewNodeId] = useState("");

  const configVaults = config?.vaults;
  const vaults = configVaults ?? [];
  const existingNames = new Set(vaults.map((s) => s.name));
  const effectiveName = newName.trim() || namePlaceholder || newType;
  const nameConflict = existingNames.has(effectiveName);
  const policies = config?.rotationPolicies ?? [];
  const retentionPolicies = config?.retentionPolicies ?? [];

  // Auto-expand a vault when navigated to from another settings tab.
  useEffect(() => {
    if (!expandTarget || !configVaults || configVaults.length === 0) return;
    const match = configVaults.find((s) => (s.name || s.id) === expandTarget);
    if (match) {
      setExpanded(match.id);
    }
    onExpandTargetConsumed?.();
  }, [expandTarget, configVaults, onExpandTargetConsumed]);

  const policyOptions = [
    { value: "", label: "(none)" },
    ...policies.map((p) => ({ value: p.id, label: p.name || p.id })),
  ];

  const defaults = (id: string) => {
    const vault = vaults.find((s) => s.id === id);
    if (!vault)
      return {
        name: "",
        policy: "",
        retentionRules: [] as RetentionRuleEdit[],
        enabled: true,
        params: {} as Record<string, string>,
        nodeId: "",
      };
    return {
      name: vault.name,
      policy: vault.policy,
      retentionRules: vault.retentionRules.map((b) => ({
        retentionPolicyId: b.retentionPolicyId,
        action: b.action,
        destinationId: b.destinationId,
      })),
      enabled: vault.enabled,
      params: { ...vault.params },
      nodeId: vault.nodeId,
    };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveVault, handleDelete } = useCrudHandlers({
    mutation: putVault,
    deleteMutation: deleteVault,
    label: "Vault",
    onSaveTransform: (
      id,
      edit: {
        name: string;
        policy: string;
        retentionRules: RetentionRuleEdit[];
        enabled: boolean;
        params: Record<string, string>;
        type: string;
        nodeId: string;
      },
    ) => ({
      id,
      name: edit.name,
      type: edit.type,
      policy: edit.policy,
      retentionRules: edit.retentionRules,
      params: edit.params,
      enabled: edit.enabled,
      nodeId: edit.nodeId,
    }),
    onDeleteTransform: (id) => ({ id, force: true }),
    clearEdit,
  });

  const clearJob = (vaultId: string) => {
    setActiveJobs((prev) => {
      const next = { ...prev };
      delete next[vaultId];
      return next;
    });
  };

  const handleCreate = async () => {
    const name = newName.trim() || namePlaceholder || newType;
    try {
      await putVault.mutateAsync({
        id: "",
        name,
        type: newType,
        policy: newPolicy,
        retentionRules: newRetentionRules,
        params: newParams,
        nodeId: newNodeId,
      });
      addToast(`Vault "${name}" created`, "info");
      setAdding(false);
      setTypeConfirmed(false);
      setNewName("");
      setNewType("memory");
      setNewPolicy("");
      setNewRetentionRules([]);
      setNewParams({});
      setNewNodeId("");
    } catch (err: any) {
      const errorMessage = err.message ?? "Failed to create vault";
      addToast(errorMessage, "error");
    }
  };

  return (
    <SettingsSection
      addLabel="Add Vault"
      adding={adding}
      onToggleAdd={() => {
        if (!adding) {
          generateName.mutateAsync().then(setNamePlaceholder);
        }
        setAdding(!adding);
        setTypeConfirmed(false);
        setNewName("");
        setNamePlaceholder("");
        setNewType("memory");
        setNewPolicy("");
        setNewRetentionRules([]);
        setNewParams({});
        setNewNodeId("");
      }}
      isLoading={isLoading}
      isEmpty={vaults.length === 0}
      emptyMessage='No vaults configured. Click "Add Vault" to create one.'
      dark={dark}
      addSlot={
        adding && !typeConfirmed ? (
          <div className="flex items-center gap-1.5">
            <SelectInput
              value=""
              onChange={(v) => {
                if (v) {
                  setNewType(v);
                  setTypeConfirmed(true);
                }
              }}
              options={[
                { value: "", label: "Select type\u2026" },
                { value: "memory", label: "memory" },
                { value: "file", label: "file" },
              ]}
              dark={dark}
            />
            <GhostButton
              onClick={() => setAdding(false)}
              dark={dark}
            >
              Cancel
            </GhostButton>
          </div>
        ) : undefined
      }
    >
      {adding && typeConfirmed && (
        <AddFormCard
          dark={dark}
          onCancel={() => {
            setAdding(false);
            setTypeConfirmed(false);
          }}
          onCreate={handleCreate}
          isPending={putVault.isPending}
          createDisabled={nameConflict}
          typeBadge={newType}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={setNewName}
              placeholder={namePlaceholder || newType}
              dark={dark}
            />
          </FormField>
          <NodeSelect value={newNodeId} onChange={setNewNodeId} dark={dark} />
          <FormField label="Rotation Policy" dark={dark}>
            <SelectInput
              value={newPolicy}
              onChange={setNewPolicy}
              options={policyOptions}
              dark={dark}
            />
          </FormField>
          <RetentionRulesEditor
            rules={newRetentionRules}
            onChange={setNewRetentionRules}
            retentionPolicies={retentionPolicies}
            vaults={vaults}
            currentVaultId=""
            dark={dark}
          />
          <VaultParamsForm
            vaultType={newType}
            params={newParams}
            onChange={setNewParams}
            dark={dark}
          />
        </AddFormCard>
      )}

      {vaults.toSorted((a, b) => a.name.localeCompare(b.name)).map((vault) => {
        const edit = getEdit(vault.id);
        const hasPolicy = vault.policy && policies.some((p) => p.id === vault.policy);
        const hasRetention = vault.retentionRules.length > 0;
        const warnings = [
          ...(!hasPolicy ? ["no rotation policy"] : []),
          ...(!hasRetention ? ["no retention policy"] : []),
        ];
        const activeJob = activeJobs[vault.id];
        return (
          <SettingsCard
            key={vault.id}
            id={vault.name || vault.id}
            typeBadge={vault.type}
            dark={dark}
            expanded={expanded === vault.id}
            onToggle={() =>
              setExpanded(expanded === vault.id ? null : vault.id)
            }
            onDelete={() => handleDelete(vault.id)}
            deleteLabel="Delete"
            footer={
              <>
                {activeJob && (
                  <JobProgress
                    jobId={activeJob.jobId}
                    label={activeJob.label}
                    dark={dark}
                    onComplete={(job) => {
                      const chunks = Number(job.chunksDone);
                      const errors = job.errorDetails.length;
                      const errorSuffix = errors > 0 ? ", " + String(errors) + " error(s)" : "";
                      addToast(
                        activeJob.label + " done: " + String(chunks) + " chunk(s)" + errorSuffix,
                        errors > 0 ? "warn" : "info",
                      );
                      clearJob(vault.id);
                    }}
                    onFailed={(job) => {
                      addToast(
                        `${activeJob.label} failed: ${job.error}`,
                        "error",
                      );
                      clearJob(vault.id);
                    }}
                  />
                )}
                <button
                  type="button"
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                    "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
                    "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
                  )}`}
                  disabled={seal.isPending || !!activeJob}
                  onClick={async () => {
                    try {
                      await seal.mutateAsync(vault.id);
                      addToast("Active chunk rotated", "info");
                    } catch (err: any) {
                      const errorMessage = err.message ?? "Rotate failed";
                      addToast(errorMessage, "error");
                    }
                  }}
                >
                  {seal.isPending ? "Rotating..." : "Rotate"}
                </button>
                <button
                  type="button"
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                    "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
                    "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
                  )}`}
                  disabled={reindex.isPending || !!activeJob}
                  onClick={async () => {
                    try {
                      const result = await reindex.mutateAsync(vault.id);
                      setActiveJobs((prev) => ({
                        ...prev,
                        [vault.id]: {
                          jobId: result.jobId,
                          label: "Reindexing",
                        },
                      }));
                    } catch (err: any) {
                      const errorMessage = err.message ?? "Reindex failed";
                      addToast(errorMessage, "error");
                    }
                  }}
                >
                  {activeJob?.label === "Reindexing"
                    ? "Reindexing..."
                    : "Reindex"}
                </button>
                {vault.enabled && (
                  <button
                    type="button"
                    className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                      "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
                      "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
                    )}`}
                    disabled={!!activeJob}
                    onClick={() => {
                      setMigrateTarget((prev) => {
                        if (prev[vault.id]) {
                          const next = { ...prev };
                          delete next[vault.id];
                          return next;
                        }
                        return { ...prev, [vault.id]: { name: "", type: "", dir: "" } };
                      });
                    }}
                  >
                    {migrateTarget[vault.id] ? "Cancel Migrate" : "Migrate"}
                  </button>
                )}
                <button
                  type="button"
                  disabled={!!activeJob}
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                    "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
                    "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
                  )}`}
                  onClick={() => {
                    setMergeTarget((prev) =>
                      prev[vault.id] !== undefined
                        ? Object.fromEntries(Object.entries(prev).filter(([k]) => k !== vault.id))
                        : { ...prev, [vault.id]: "" },
                    );
                  }}
                >
                  {mergeTarget[vault.id] !== undefined ? "Cancel Merge" : "Merge Into..."}
                </button>
                <PrimaryButton
                  onClick={() =>
                    saveVault(vault.id, {
                      ...getEdit(vault.id),
                      type: vault.type,
                    })
                  }
                  disabled={putVault.isPending || !isDirty(vault.id)}
                >
                  {putVault.isPending ? "Saving..." : "Save"}
                </PrimaryButton>
              </>
            }
            headerRight={
              <span className="flex items-center gap-2">
                <NodeBadge nodeId={vault.nodeId} dark={dark} />
                {!vault.enabled && (
                  <Badge variant="ghost" dark={dark}>disabled</Badge>
                )}
                {warnings.length > 0 && (
                  <span className="text-[0.85em] text-severity-warn">
                    {warnings.join(", ")}
                  </span>
                )}
              </span>
            }
          >
            <div className="flex flex-col gap-3">
              <FormField label="Name" dark={dark}>
                <TextInput
                  value={edit.name}
                  onChange={(v) => setEdit(vault.id, { name: v })}
                  dark={dark}
                />
              </FormField>
              <Checkbox
                checked={edit.enabled}
                onChange={(v) => setEdit(vault.id, { enabled: v })}
                label="Enabled"
                dark={dark}
              />
              <NodeSelect
                value={edit.nodeId}
                onChange={(v) => setEdit(vault.id, { nodeId: v })}
                dark={dark}
              />
              <FormField label="Rotation Policy" dark={dark}>
                <SelectInput
                  value={edit.policy}
                  onChange={(v) => setEdit(vault.id, { policy: v })}
                  options={policyOptions}
                  dark={dark}
                />
              </FormField>
              <RetentionRulesEditor
                rules={edit.retentionRules}
                onChange={(rules) =>
                  setEdit(vault.id, { retentionRules: rules })
                }
                retentionPolicies={retentionPolicies}
                vaults={vaults}
                currentVaultId={vault.id}
                dark={dark}
              />
              <VaultParamsForm
                vaultType={vault.type}
                params={edit.params}
                onChange={(p) => setEdit(vault.id, { params: p })}
                dark={dark}
              />
              {migrateTarget[vault.id] && (() => {
                const mt = migrateTarget[vault.id]!;
                const resolvedType = mt.type || vault.type;
                const dirRequired = resolvedType === "file";
                const canSubmit = mt.name.trim() && (!dirRequired || mt.dir.trim());
                return (
                  <div
                    className={`flex flex-col gap-3 p-3 rounded border ${c(
                      "border-ink-border-subtle bg-ink-raised",
                      "border-light-border-subtle bg-light-bg",
                    )}`}
                  >
                    <div
                      className={`text-[0.75em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      Migrate Vault
                    </div>
                    <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                      Creates a new destination vault, disables this vault so no new data flows in, then moves all records to the destination and deletes this vault.
                    </p>
                    <div className="grid grid-cols-3 gap-3">
                      <FormField label="Destination Name" dark={dark}>
                        <TextInput
                          value={mt.name}
                          onChange={(v) =>
                            setMigrateTarget((prev) => ({
                              ...prev,
                              [vault.id]: { ...prev[vault.id]!, name: v },
                            }))
                          }
                          placeholder="new-vault"
                          dark={dark}
                          mono
                        />
                      </FormField>
                      <FormField label="Type" dark={dark}>
                        <SelectInput
                          value={mt.type}
                          onChange={(v) =>
                            setMigrateTarget((prev) => ({
                              ...prev,
                              [vault.id]: { ...prev[vault.id]!, type: v, dir: "" },
                            }))
                          }
                          options={[
                            { value: "", label: `same (${vault.type})` },
                            { value: "memory", label: "memory" },
                            { value: "file", label: "file" },
                          ]}
                          dark={dark}
                        />
                      </FormField>
                      {dirRequired && (
                        <FormField label="Directory" dark={dark}>
                          <TextInput
                            value={mt.dir}
                            onChange={(v) =>
                              setMigrateTarget((prev) => ({
                                ...prev,
                                [vault.id]: { ...prev[vault.id]!, dir: v },
                              }))
                            }
                            placeholder="/path/to/vault"
                            dark={dark}
                            mono
                            examples={["/var/lib/gastrolog/data"]}
                          />
                        </FormField>
                      )}
                    </div>
                    <div className="flex justify-end">
                      <PrimaryButton
                        disabled={migrate.isPending || !canSubmit || !!activeJob}
                        onClick={async () => {
                          const trimmedName = mt.name.trim();
                          if (!trimmedName) return;
                          const srcLabel = vault.name || vault.id;
                          if (!confirm(`Migrate "${srcLabel}" to "${trimmedName}"? This will immediately disable "${srcLabel}" and delete it after all records are moved.`)) return;
                          const destType = mt.type || undefined;
                          const params: Record<string, string> = {};
                          if (mt.dir.trim()) {
                            params["dir"] = mt.dir.trim();
                          }
                          const destParams = Object.keys(params).length > 0 ? params : undefined;
                          try {
                            const result = await migrate.mutateAsync({
                              source: vault.id,
                              destination: trimmedName,
                              destinationType: destType,
                              destinationParams: destParams,
                            });
                            setActiveJobs((prev) => ({
                              ...prev,
                              [vault.id]: {
                                jobId: result.jobId,
                                label: "Migrating",
                              },
                            }));
                            setMigrateTarget((prev) => {
                              const next = { ...prev };
                              delete next[vault.id];
                              return next;
                            });
                          } catch (err: any) {
                            const errorMessage = err.message ?? "Migrate failed";
                            addToast(errorMessage, "error");
                          }
                        }}
                      >
                        {migrate.isPending ? "Migrating..." : "Migrate"}
                      </PrimaryButton>
                    </div>
                  </div>
                );
              })()}
              {mergeTarget[vault.id] !== undefined && (
                <div
                  className={`flex flex-col gap-3 p-3 rounded border ${c(
                    "border-ink-border-subtle bg-ink-raised",
                    "border-light-border-subtle bg-light-bg",
                  )}`}
                >
                  <div
                    className={`text-[0.75em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                  >
                    Merge Into Another Vault
                  </div>
                  <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                    Disables this vault, moves all records into the destination, then deletes this vault.
                  </p>
                  <div className="grid grid-cols-2 gap-3">
                    <FormField label="Destination" dark={dark}>
                      <SelectInput
                        value={mergeTarget[vault.id] ?? ""}
                        onChange={(v) =>
                          setMergeTarget((prev) => ({ ...prev, [vault.id]: v }))
                        }
                        options={[
                          { value: "", label: "(select)" },
                          ...vaults
                            .filter((s) => s.id !== vault.id)
                            .map((s) => ({ value: s.id, label: s.name || s.id })),
                        ]}
                        dark={dark}
                      />
                    </FormField>
                  </div>
                  <div className="flex justify-end">
                    <PrimaryButton
                      disabled={merge.isPending || !mergeTarget[vault.id] || !!activeJob}
                      onClick={async () => {
                        const dest = mergeTarget[vault.id];
                        if (!dest) return;
                        const destName = vaults.find((s) => s.id === dest)?.name || dest;
                        if (!confirm(`Merge "${vault.name || vault.id}" into "${destName}"? This will immediately disable "${vault.name || vault.id}" and delete it after all records are moved.`)) return;
                        try {
                          const result = await merge.mutateAsync({
                            source: vault.id,
                            destination: dest,
                          });
                          setActiveJobs((prev) => ({
                            ...prev,
                            [vault.id]: {
                              jobId: result.jobId,
                              label: "Merging",
                            },
                          }));
                          setMergeTarget((prev) =>
                            Object.fromEntries(Object.entries(prev).filter(([k]) => k !== vault.id)),
                          );
                        } catch (err: any) {
                          const errorMessage = err.message ?? "Merge failed";
                          addToast(errorMessage, "error");
                        }
                      }}
                    >
                      {merge.isPending ? "Merging..." : "Merge"}
                    </PrimaryButton>
                  </div>
                </div>
              )}
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}
