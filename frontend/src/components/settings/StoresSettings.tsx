import { useState, useCallback } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import {
  useConfig,
  usePutStore,
  useDeleteStore,
  useReindexStore,
  useCloneStore,
  useMergeStores,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { StoreParamsForm } from "./StoreParamsForm";
import { PrimaryButton } from "./Buttons";
import { Checkbox } from "./Checkbox";

export function StoresSettings({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putStore = usePutStore();
  const deleteStore = useDeleteStore();
  const reindex = useReindexStore();
  const clone = useCloneStore();
  const merge = useMergeStores();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [cloneTarget, setCloneTarget] = useState<
    Record<string, { name: string; dir: string }>
  >({});
  const [mergeTarget, setMergeTarget] = useState<Record<string, string>>({});

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

  const defaults = useCallback(
    (id: string) => {
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
    },
    [stores],
  );

  const { getEdit, setEdit, clearEdit } = useEditState(defaults);

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
      addLabel="Add Store"
      adding={adding}
      onToggleAdd={() => setAdding(!adding)}
      isLoading={isLoading}
      isEmpty={stores.length === 0}
      emptyMessage='No stores configured. Click "Add Store" to create one.'
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => setAdding(false)}
          onCreate={handleCreate}
          isPending={putStore.isPending}
        >
          <div className="grid grid-cols-2 gap-3">
            <FormField label="Name" dark={dark}>
              <TextInput
                value={newName}
                onChange={setNewName}
                placeholder="my-store"
                dark={dark}
              />
            </FormField>
            <FormField label="Type" dark={dark}>
              <SelectInput
                value={newType}
                onChange={setNewType}
                options={[
                  { value: "memory", label: "memory" },
                  { value: "file", label: "file" },
                ]}
                dark={dark}
              />
            </FormField>
          </div>
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
                <button
                  type="button"
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                    "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
                    "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
                  )}`}
                  disabled={reindex.isPending}
                  onClick={async () => {
                    try {
                      const result = await reindex.mutateAsync(store.id);
                      addToast(
                        `Reindexed ${result.chunksReindexed} chunk(s)${result.errors > 0 ? `, ${result.errors} error(s)` : ""}`,
                        result.errors > 0 ? "warn" : "info",
                      );
                    } catch (err: any) {
                      addToast(err.message ?? "Reindex failed", "error");
                    }
                  }}
                >
                  {reindex.isPending ? "Reindexing..." : "Reindex"}
                </button>
                <button
                  type="button"
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                    "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
                    "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
                  )}`}
                  onClick={() => {
                    setCloneTarget((prev) => {
                      if (prev[store.id]) {
                        const next = { ...prev };
                        delete next[store.id];
                        return next;
                      }
                      return { ...prev, [store.id]: { name: "", dir: "" } };
                    });
                  }}
                >
                  {cloneTarget[store.id] ? "Cancel Clone" : "Clone"}
                </button>
                <button
                  type="button"
                  disabled
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors opacity-50 cursor-not-allowed ${c(
                    "border-ink-border-subtle text-text-muted",
                    "border-light-border-subtle text-light-text-muted",
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
                  disabled={putStore.isPending}
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
              {cloneTarget[store.id] && (
                <div
                  className={`flex flex-col gap-3 p-3 rounded border ${c(
                    "border-ink-border-subtle bg-ink-raised",
                    "border-light-border-subtle bg-light-bg",
                  )}`}
                >
                  <div
                    className={`text-[0.75em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                  >
                    Clone Store
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <FormField label="Name" dark={dark}>
                      <TextInput
                        value={cloneTarget[store.id].name}
                        onChange={(v) =>
                          setCloneTarget((prev) => ({
                            ...prev,
                            [store.id]: { ...prev[store.id], name: v },
                          }))
                        }
                        placeholder="cloned-store"
                        dark={dark}
                        mono
                      />
                    </FormField>
                    {store.type === "file" && (
                      <FormField label="Directory (optional)" dark={dark}>
                        <TextInput
                          value={cloneTarget[store.id].dir}
                          onChange={(v) =>
                            setCloneTarget((prev) => ({
                              ...prev,
                              [store.id]: { ...prev[store.id], dir: v },
                            }))
                          }
                          placeholder="auto-derived from name"
                          dark={dark}
                          mono
                        />
                      </FormField>
                    )}
                  </div>
                  <div className="flex justify-end">
                    <PrimaryButton
                      disabled={
                        clone.isPending ||
                        !cloneTarget[store.id].name.trim()
                      }
                      onClick={async () => {
                        const { name, dir } = cloneTarget[store.id];
                        const trimmedName = name.trim();
                        if (!trimmedName) return;
                        try {
                          const params: Record<string, string> = {};
                          if (dir.trim()) {
                            params["dir"] = dir.trim();
                          }
                          const result = await clone.mutateAsync({
                            source: store.id,
                            destination: trimmedName,
                            destinationParams:
                              Object.keys(params).length > 0
                                ? params
                                : undefined,
                          });
                          addToast(
                            `Cloned ${result.recordsCopied} record(s) to "${trimmedName}"`,
                            "info",
                          );
                          setCloneTarget((prev) => {
                            const next = { ...prev };
                            delete next[store.id];
                            return next;
                          });
                        } catch (err: any) {
                          addToast(
                            err.message ?? "Clone failed",
                            "error",
                          );
                        }
                      }}
                    >
                      {clone.isPending ? "Cloning..." : "Clone"}
                    </PrimaryButton>
                  </div>
                </div>
              )}
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
                  <div className="grid grid-cols-2 gap-3">
                    <FormField label="Destination" dark={dark}>
                      <SelectInput
                        value={mergeTarget[store.id]}
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
                      disabled={merge.isPending || !mergeTarget[store.id]}
                      onClick={async () => {
                        const dest = mergeTarget[store.id];
                        if (!dest) return;
                        const destName = stores.find((s) => s.id === dest)?.name || dest;
                        if (!confirm(`Merge all records from "${store.name || store.id}" into "${destName}"? This will delete "${store.name || store.id}" afterward.`)) return;
                        try {
                          const result = await merge.mutateAsync({
                            source: store.id,
                            destination: dest,
                          });
                          addToast(
                            `Merged ${result.recordsMerged} record(s) into "${destName}"`,
                            "info",
                          );
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
