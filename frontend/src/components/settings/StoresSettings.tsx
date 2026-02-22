import { useState, useEffect, useRef, useCallback } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import {
  useConfig,
  usePutStore,
  useDeleteStore,
  useSealStore,
  useReindexStore,
  useMigrateStore,
  useMergeStores,
  useJob,
} from "../../api/hooks";
import { JobStatus } from "../../api/client";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { StoreParamsForm } from "./StoreParamsForm";
import { PrimaryButton, GhostButton } from "./Buttons";
import { Checkbox } from "./Checkbox";
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
      qc.invalidateQueries({ queryKey: ["stores"] });
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
  stores,
  currentStoreId,
  dark,
}: Readonly<{
  rules: RetentionRuleEdit[];
  onChange: (rules: RetentionRuleEdit[]) => void;
  retentionPolicies: Array<{ id: string; name: string }>;
  stores: Array<{ id: string; name: string }>;
  currentStoreId: string;
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
  const storeOptions = [
    { value: "", label: "(select store)" },
    ...stores
      .filter((s) => s.id !== currentStoreId)
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
                  options={storeOptions}
                  dark={dark}
                />
              </FormField>
            </div>
          )}
          <button
            type="button"
            onClick={() => onChange(rules.filter((_, i) => i !== idx))}
            className={`pb-1.5 text-[0.85em] transition-colors ${c(
              "text-text-ghost hover:text-severity-error",
              "text-light-text-ghost hover:text-severity-error",
            )}`}
          >
            &times;
          </button>
        </div>
      ))}
    </div>
  );
}

export function StoresSettings({ dark, expandTarget, onExpandTargetConsumed }: Readonly<{ dark: boolean; expandTarget?: string | null; onExpandTargetConsumed?: () => void }>) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putStore = usePutStore();
  const deleteStore = useDeleteStore();
  const seal = useSealStore();
  const reindex = useReindexStore();
  const migrate = useMigrateStore();
  const merge = useMergeStores();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [typeConfirmed, setTypeConfirmed] = useState(false);
  const [migrateTarget, setMigrateTarget] = useState<
    Record<string, { name: string; type: string; dir: string }>
  >({});
  const [mergeTarget, setMergeTarget] = useState<Record<string, string>>({});
  // Track active jobs per store: { storeId: { jobId, label } }
  const [activeJobs, setActiveJobs] = useState<
    Record<string, { jobId: string; label: string }>
  >({});

  // New store form state.
  const [newName, setNewName] = useState("");
  const [newType, setNewType] = useState("memory");
  const [newFilter, setNewFilter] = useState("");
  const [newPolicy, setNewPolicy] = useState("");
  const [newRetentionRules, setNewRetentionRules] = useState<RetentionRuleEdit[]>([]);
  const [newParams, setNewParams] = useState<Record<string, string>>({});

  const stores = config?.stores ?? [];
  const existingNames = new Set(stores.map((s) => s.name));
  const effectiveName = newName.trim() || newType;
  const nameConflict = existingNames.has(effectiveName);
  const policies = config?.rotationPolicies ?? [];
  const retentionPolicies = config?.retentionPolicies ?? [];
  const filters = config?.filters ?? [];

  // Auto-expand a store when navigated to from another settings tab.
  useEffect(() => {
    if (!expandTarget || stores.length === 0) return;
    const match = stores.find((s) => (s.name || s.id) === expandTarget);
    if (match) {
      setExpanded(match.id);
    }
    onExpandTargetConsumed?.();
  }, [expandTarget, stores, onExpandTargetConsumed]);

  const filterOptions = [
    { value: "", label: "(none)" },
    ...filters.map((f) => ({ value: f.id, label: f.name || f.id })),
  ];

  const policyOptions = [
    { value: "", label: "(none)" },
    ...policies.map((p) => ({ value: p.id, label: p.name || p.id })),
  ];

  const defaults = (id: string) => {
    const store = stores.find((s) => s.id === id);
    if (!store)
      return {
        name: "",
        filter: "",
        policy: "",
        retentionRules: [] as RetentionRuleEdit[],
        enabled: true,
        params: {} as Record<string, string>,
      };
    return {
      name: store.name,
      filter: store.filter,
      policy: store.policy,
      retentionRules: (store.retentionRules ?? []).map((b) => ({
        retentionPolicyId: b.retentionPolicyId,
        action: b.action,
        destinationId: b.destinationId,
      })),
      enabled: store.enabled,
      params: { ...store.params },
    };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveStore, handleDelete } = useCrudHandlers({
    mutation: putStore,
    deleteMutation: deleteStore,
    label: "Store",
    onSaveTransform: (
      id,
      edit: {
        name: string;
        filter: string;
        policy: string;
        retentionRules: RetentionRuleEdit[];
        enabled: boolean;
        params: Record<string, string>;
        type: string;
      },
    ) => ({
      id,
      name: edit.name,
      type: edit.type,
      filter: edit.filter,
      policy: edit.policy,
      retentionRules: edit.retentionRules,
      params: edit.params,
      enabled: edit.enabled,
    }),
    onDeleteTransform: (id) => ({ id, force: true }),
    clearEdit,
  });

  const clearJob = (storeId: string) => {
    setActiveJobs((prev) => {
      const next = { ...prev };
      delete next[storeId];
      return next;
    });
  };

  const handleCreate = async () => {
    const name = newName.trim() || newType;
    try {
      await putStore.mutateAsync({
        id: "",
        name,
        type: newType,
        filter: newFilter,
        policy: newPolicy,
        retentionRules: newRetentionRules,
        params: newParams,
      });
      addToast(`Store "${name}" created`, "info");
      setAdding(false);
      setTypeConfirmed(false);
      setNewName("");
      setNewType("memory");
      setNewFilter("");
      setNewPolicy("");
      setNewRetentionRules([]);
      setNewParams({});
    } catch (err: any) {
      const errorMessage = err.message ?? "Failed to create store";
      addToast(errorMessage, "error");
    }
  };

  return (
    <SettingsSection
      title="Stores"
      helpTopicId="storage-engines"
      addLabel="Add Store"
      adding={adding}
      onToggleAdd={() => {
        setAdding(!adding);
        setTypeConfirmed(false);
        setNewName("");
        setNewType("memory");
        setNewFilter("");
        setNewPolicy("");
        setNewRetentionRules([]);
        setNewParams({});
      }}
      isLoading={isLoading}
      isEmpty={stores.length === 0}
      emptyMessage='No stores configured. Click "Add Store" to create one.'
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
          isPending={putStore.isPending}
          createDisabled={nameConflict}
          typeBadge={newType}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={setNewName}
              placeholder={newType}
              dark={dark}
            />
          </FormField>
          <div className="grid grid-cols-2 gap-3">
            <FormField label="Filter" dark={dark}>
              <SelectInput
                value={newFilter}
                onChange={setNewFilter}
                options={filterOptions}
                dark={dark}
              />
            </FormField>
            <FormField label="Rotation Policy" dark={dark}>
              <SelectInput
                value={newPolicy}
                onChange={setNewPolicy}
                options={policyOptions}
                dark={dark}
              />
            </FormField>
          </div>
          <RetentionRulesEditor
            rules={newRetentionRules}
            onChange={setNewRetentionRules}
            retentionPolicies={retentionPolicies}
            stores={stores}
            currentStoreId=""
            dark={dark}
          />
          <StoreParamsForm
            storeType={newType}
            params={newParams}
            onChange={setNewParams}
            dark={dark}
          />
        </AddFormCard>
      )}

      {stores.map((store) => {
        const edit = getEdit(store.id);
        const hasPolicy = store.policy && policies.some((p) => p.id === store.policy);
        const hasFilter = store.filter && filters.some((f) => f.id === store.filter);
        const hasRetention = (store.retentionRules ?? []).length > 0;
        const warnings = [
          ...(!hasPolicy ? ["no rotation policy"] : []),
          ...(!hasRetention ? ["no retention policy"] : []),
          ...(!hasFilter ? ["no filter"] : []),
        ];
        const activeJob = activeJobs[store.id];
        return (
          <SettingsCard
            key={store.id}
            id={store.name || store.id}
            typeBadge={store.type}
            dark={dark}
            expanded={expanded === store.id}
            onToggle={() =>
              setExpanded(expanded === store.id ? null : store.id)
            }
            onDelete={() => handleDelete(store.id)}
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
                      addToast(
                        `${activeJob.label} done: ${chunks} chunk(s)${errors > 0 ? `, ${errors} error(s)` : ""}`,
                        errors > 0 ? "warn" : "info",
                      );
                      clearJob(store.id);
                    }}
                    onFailed={(job) => {
                      addToast(
                        `${activeJob.label} failed: ${job.error}`,
                        "error",
                      );
                      clearJob(store.id);
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
                      await seal.mutateAsync(store.id);
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
                      const result = await reindex.mutateAsync(store.id);
                      setActiveJobs((prev) => ({
                        ...prev,
                        [store.id]: {
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
                {store.enabled && (
                  <button
                    type="button"
                    className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                      "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
                      "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
                    )}`}
                    disabled={!!activeJob}
                    onClick={() => {
                      setMigrateTarget((prev) => {
                        if (prev[store.id]) {
                          const next = { ...prev };
                          delete next[store.id];
                          return next;
                        }
                        return { ...prev, [store.id]: { name: "", type: "", dir: "" } };
                      });
                    }}
                  >
                    {migrateTarget[store.id] ? "Cancel Migrate" : "Migrate"}
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
                      prev[store.id] !== undefined
                        ? Object.fromEntries(Object.entries(prev).filter(([k]) => k !== store.id))
                        : { ...prev, [store.id]: "" },
                    );
                  }}
                >
                  {mergeTarget[store.id] !== undefined ? "Cancel Merge" : "Merge Into..."}
                </button>
                <PrimaryButton
                  onClick={() =>
                    saveStore(store.id, {
                      ...getEdit(store.id),
                      type: store.type,
                    })
                  }
                  disabled={putStore.isPending || !isDirty(store.id)}
                >
                  {putStore.isPending ? "Saving..." : "Save"}
                </PrimaryButton>
              </>
            }
            headerRight={
              <span className="flex items-center gap-2">
                {!store.enabled && (
                  <span
                    className={`px-1.5 py-0.5 text-[0.8em] font-mono rounded ${c(
                      "bg-ink-hover text-text-ghost",
                      "bg-light-hover text-light-text-ghost",
                    )}`}
                  >
                    disabled
                  </span>
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
                  onChange={(v) => setEdit(store.id, { name: v })}
                  dark={dark}
                />
              </FormField>
              <Checkbox
                checked={edit.enabled}
                onChange={(v) => setEdit(store.id, { enabled: v })}
                label="Enabled"
                dark={dark}
              />
              <div className="grid grid-cols-2 gap-3">
                <FormField label="Filter" dark={dark}>
                  <SelectInput
                    value={edit.filter}
                    onChange={(v) => setEdit(store.id, { filter: v })}
                    options={filterOptions}
                    dark={dark}
                  />
                </FormField>
                <FormField label="Rotation Policy" dark={dark}>
                  <SelectInput
                    value={edit.policy}
                    onChange={(v) => setEdit(store.id, { policy: v })}
                    options={policyOptions}
                    dark={dark}
                  />
                </FormField>
              </div>
              <RetentionRulesEditor
                rules={edit.retentionRules}
                onChange={(rules) =>
                  setEdit(store.id, { retentionRules: rules })
                }
                retentionPolicies={retentionPolicies}
                stores={stores}
                currentStoreId={store.id}
                dark={dark}
              />
              <StoreParamsForm
                storeType={store.type}
                params={edit.params}
                onChange={(p) => setEdit(store.id, { params: p })}
                dark={dark}
              />
              {migrateTarget[store.id] && (() => {
                const mt = migrateTarget[store.id]!;
                const resolvedType = mt.type || store.type;
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
                      Migrate Store
                    </div>
                    <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                      Creates a new destination store, disables this store so no new data flows in, then moves all records to the destination and deletes this store.
                    </p>
                    <div className="grid grid-cols-3 gap-3">
                      <FormField label="Destination Name" dark={dark}>
                        <TextInput
                          value={mt.name}
                          onChange={(v) =>
                            setMigrateTarget((prev) => ({
                              ...prev,
                              [store.id]: { ...prev[store.id]!, name: v },
                            }))
                          }
                          placeholder="new-store"
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
                              [store.id]: { ...prev[store.id]!, type: v, dir: "" },
                            }))
                          }
                          options={[
                            { value: "", label: `same (${store.type})` },
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
                                [store.id]: { ...prev[store.id]!, dir: v },
                              }))
                            }
                            placeholder="/path/to/store"
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
                          const srcLabel = store.name || store.id;
                          if (!confirm(`Migrate "${srcLabel}" to "${trimmedName}"? This will immediately disable "${srcLabel}" and delete it after all records are moved.`)) return;
                          const destType = mt.type || undefined;
                          const params: Record<string, string> = {};
                          if (mt.dir.trim()) {
                            params["dir"] = mt.dir.trim();
                          }
                          const destParams = Object.keys(params).length > 0 ? params : undefined;
                          try {
                            const result = await migrate.mutateAsync({
                              source: store.id,
                              destination: trimmedName,
                              destinationType: destType,
                              destinationParams: destParams,
                            });
                            setActiveJobs((prev) => ({
                              ...prev,
                              [store.id]: {
                                jobId: result.jobId,
                                label: "Migrating",
                              },
                            }));
                            setMigrateTarget((prev) => {
                              const next = { ...prev };
                              delete next[store.id];
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
              {mergeTarget[store.id] !== undefined && (
                <div
                  className={`flex flex-col gap-3 p-3 rounded border ${c(
                    "border-ink-border-subtle bg-ink-raised",
                    "border-light-border-subtle bg-light-bg",
                  )}`}
                >
                  <div
                    className={`text-[0.75em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                  >
                    Merge Into Another Store
                  </div>
                  <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                    Disables this store, moves all records into the destination, then deletes this store.
                  </p>
                  <div className="grid grid-cols-2 gap-3">
                    <FormField label="Destination" dark={dark}>
                      <SelectInput
                        value={mergeTarget[store.id] ?? ""}
                        onChange={(v) =>
                          setMergeTarget((prev) => ({ ...prev, [store.id]: v }))
                        }
                        options={[
                          { value: "", label: "(select)" },
                          ...stores
                            .filter((s) => s.id !== store.id)
                            .map((s) => ({ value: s.id, label: s.name || s.id })),
                        ]}
                        dark={dark}
                      />
                    </FormField>
                  </div>
                  <div className="flex justify-end">
                    <PrimaryButton
                      disabled={merge.isPending || !mergeTarget[store.id] || !!activeJob}
                      onClick={async () => {
                        const dest = mergeTarget[store.id];
                        if (!dest) return;
                        const destName = stores.find((s) => s.id === dest)?.name || dest;
                        if (!confirm(`Merge "${store.name || store.id}" into "${destName}"? This will immediately disable "${store.name || store.id}" and delete it after all records are moved.`)) return;
                        try {
                          const result = await merge.mutateAsync({
                            source: store.id,
                            destination: dest,
                          });
                          setActiveJobs((prev) => ({
                            ...prev,
                            [store.id]: {
                              jobId: result.jobId,
                              label: "Merging",
                            },
                          }));
                          setMergeTarget((prev) =>
                            Object.fromEntries(Object.entries(prev).filter(([k]) => k !== store.id)),
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
