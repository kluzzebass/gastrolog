import { useState, useEffect } from "react";
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
  const [handled, setHandled] = useState(false);

  useEffect(() => {
    if (!job || handled) return;
    if (job.status === JobStatus.COMPLETED) {
      setHandled(true);
      qc.invalidateQueries({ queryKey: ["stores"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      qc.invalidateQueries({ queryKey: ["indexes"] });
      qc.invalidateQueries({ queryKey: ["config"] });
      onComplete(job);
    } else if (job.status === JobStatus.FAILED) {
      setHandled(true);
      onFailed(job);
    }
  }, [job, handled, onComplete, onFailed, qc]);

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

export function StoresSettings({ dark }: Readonly<{ dark: boolean }>) {
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
  const [newRetention, setNewRetention] = useState("");
  const [newParams, setNewParams] = useState<Record<string, string>>({});

  const stores = config?.stores ?? [];
  const policies = config?.rotationPolicies ?? [];
  const retentionPolicies = config?.retentionPolicies ?? [];
  const filters = config?.filters ?? [];

  const filterOptions = [
    { value: "", label: "(none)" },
    ...filters.map((f) => ({ value: f.id, label: f.name || f.id })),
  ];

  const policyOptions = [
    { value: "", label: "(none)" },
    ...policies.map((p) => ({ value: p.id, label: p.name || p.id })),
  ];

  const retentionOptions = [
    { value: "", label: "(none)" },
    ...retentionPolicies.map((r) => ({ value: r.id, label: r.name || r.id })),
  ];

  const defaults = (id: string) => {
    const store = stores.find((s) => s.id === id);
    if (!store)
      return {
        filter: "",
        policy: "",
        retention: "",
        enabled: true,
        params: {} as Record<string, string>,
      };
    return {
      filter: store.filter,
      policy: store.policy,
      retention: store.retention,
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
        filter: string;
        policy: string;
        retention: string;
        enabled: boolean;
        params: Record<string, string>;
        type: string;
      },
    ) => ({
      id,
      name: stores.find((s) => s.id === id)?.name ?? "",
      type: edit.type,
      filter: edit.filter,
      policy: edit.policy,
      retention: edit.retention,
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
    if (!newName.trim()) {
      addToast("Store name is required", "warn");
      return;
    }
    try {
      await putStore.mutateAsync({
        id: "",
        name: newName.trim(),
        type: newType,
        filter: newFilter,
        policy: newPolicy,
        retention: newRetention,
        params: newParams,
      });
      addToast(`Store "${newName.trim()}" created`, "info");
      setAdding(false);
      setTypeConfirmed(false);
      setNewName("");
      setNewType("memory");
      setNewFilter("");
      setNewPolicy("");
      setNewRetention("");
      setNewParams({});
    } catch (err: any) {
      addToast(err.message ?? "Failed to create store", "error");
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
        setNewRetention("");
        setNewParams({});
      }}
      isLoading={isLoading}
      isEmpty={stores.length === 0}
      emptyMessage='No stores configured. Click "Add Store" to create one.'
      dark={dark}
      addSlot={
        adding && !typeConfirmed ? (
          <div className="flex items-center gap-1.5">
            {[
              { value: "memory", label: "memory" },
              { value: "file", label: "file" },
            ].map((t) => (
              <button
                key={t.value}
                type="button"
                onClick={() => {
                  setNewType(t.value);
                  setTypeConfirmed(true);
                }}
                className={`px-3 py-1.5 text-[0.8em] font-mono rounded border transition-colors ${c(
                  "border-ink-border-subtle text-text-secondary hover:border-copper hover:text-copper",
                  "border-light-border-subtle text-light-text-secondary hover:border-copper hover:text-copper",
                )}`}
              >
                {t.label}
              </button>
            ))}
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
          typeBadge={newType}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={setNewName}
              placeholder="my-store"
              dark={dark}
            />
          </FormField>
          <div className="grid grid-cols-3 gap-3">
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
            <FormField label="Retention Policy" dark={dark}>
              <SelectInput
                value={newRetention}
                onChange={setNewRetention}
                options={retentionOptions}
                dark={dark}
              />
            </FormField>
          </div>
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
        const hasRetention =
          store.retention && retentionPolicies.some((r) => r.id === store.retention);
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
                      addToast(err.message ?? "Rotate failed", "error");
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
                      addToast(err.message ?? "Reindex failed", "error");
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
              <Checkbox
                checked={edit.enabled}
                onChange={(v) => setEdit(store.id, { enabled: v })}
                label="Enabled"
                dark={dark}
              />
              <div className="grid grid-cols-3 gap-3">
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
                <FormField label="Retention Policy" dark={dark}>
                  <SelectInput
                    value={edit.retention}
                    onChange={(v) => setEdit(store.id, { retention: v })}
                    options={retentionOptions}
                    dark={dark}
                  />
                </FormField>
              </div>
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
                          try {
                            const params: Record<string, string> = {};
                            if (mt.dir.trim()) {
                              params["dir"] = mt.dir.trim();
                            }
                            const result = await migrate.mutateAsync({
                              source: store.id,
                              destination: trimmedName,
                              destinationType: mt.type || undefined,
                              destinationParams:
                                Object.keys(params).length > 0 ? params : undefined,
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
                            addToast(err.message ?? "Migrate failed", "error");
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
                          addToast(err.message ?? "Merge failed", "error");
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
