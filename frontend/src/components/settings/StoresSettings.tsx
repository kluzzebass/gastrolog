import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useConfig, usePutStore, useDeleteStore } from "../../api/hooks";
import { useToast } from "../Toast";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { StoreParamsForm } from "./StoreParamsForm";

export function StoresSettings({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putStore = usePutStore();
  const deleteStore = useDeleteStore();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  // Edit state per store.
  const [edits, setEdits] = useState<
    Record<
      string,
      {
        filter: string;
        policy: string;
        retention: string;
        params: Record<string, string>;
      }
    >
  >({});

  // New store form state.
  const [newId, setNewId] = useState("");
  const [newType, setNewType] = useState("memory");
  const [newFilter, setNewFilter] = useState("");
  const [newPolicy, setNewPolicy] = useState("");
  const [newRetention, setNewRetention] = useState("");
  const [newParams, setNewParams] = useState<Record<string, string>>({});

  const stores = config?.stores ?? [];
  const policies = config?.rotationPolicies ?? {};
  const retentionPolicies = config?.retentionPolicies ?? {};
  const filters = config?.filters ?? {};

  const filterOptions = [
    { value: "", label: "(none)" },
    ...Object.keys(filters).map((id) => ({ value: id, label: id })),
  ];

  const policyOptions = [
    { value: "", label: "(none)" },
    ...Object.keys(policies).map((id) => ({ value: id, label: id })),
  ];

  const retentionOptions = [
    { value: "", label: "(none)" },
    ...Object.keys(retentionPolicies).map((id) => ({ value: id, label: id })),
  ];

  const getEdit = (store: {
    id: string;
    filter: string;
    policy: string;
    retention: string;
    params: Record<string, string>;
  }) => {
    const existing = edits[store.id];
    if (existing) return existing;
    return {
      filter: store.filter,
      policy: store.policy,
      retention: store.retention,
      params: { ...store.params },
    };
  };

  const setEdit = (
    id: string,
    patch: Partial<{
      filter: string;
      policy: string;
      retention: string;
      params: Record<string, string>;
    }>,
  ) => {
    const store = stores.find((s) => s.id === id)!;
    const current = getEdit(store);
    setEdits((prev) => ({
      ...prev,
      [id]: { ...current, ...patch },
    }));
  };

  const handleSave = async (id: string, type: string) => {
    const store = stores.find((s) => s.id === id)!;
    const edit = getEdit(store);
    try {
      await putStore.mutateAsync({
        id,
        type,
        filter: edit.filter,
        policy: edit.policy,
        retention: edit.retention,
        params: edit.params,
      });
      setEdits((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      addToast(`Store "${id}" updated`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update store", "error");
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteStore.mutateAsync(id);
      addToast(`Store "${id}" deleted`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to delete store", "error");
    }
  };

  const handleCreate = async () => {
    if (!newId.trim()) {
      addToast("Store ID is required", "warn");
      return;
    }
    try {
      await putStore.mutateAsync({
        id: newId.trim(),
        type: newType,
        filter: newFilter,
        policy: newPolicy,
        retention: newRetention,
        params: newParams,
      });
      addToast(`Store "${newId.trim()}" created`, "info");
      setAdding(false);
      setNewId("");
      setNewType("memory");
      setNewFilter("");
      setNewPolicy("");
      setNewRetention("");
      setNewParams({});
    } catch (err: any) {
      addToast(err.message ?? "Failed to create store", "error");
    }
  };

  if (isLoading) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading...
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Stores
        </h2>
        <button
          onClick={() => setAdding(!adding)}
          className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors"
        >
          {adding ? "Cancel" : "Add Store"}
        </button>
      </div>

      <div className="flex flex-col gap-3">
        {/* New store form */}
        {adding && (
          <div
            className={`border rounded-lg p-4 ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
          >
            <div className="flex flex-col gap-3">
              <div className="grid grid-cols-2 gap-3">
                <FormField label="ID" dark={dark}>
                  <TextInput
                    value={newId}
                    onChange={setNewId}
                    placeholder="my-store"
                    dark={dark}
                    mono
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
              <div className="flex justify-end gap-2 pt-2">
                <button
                  onClick={() => setAdding(false)}
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                    "border-ink-border text-text-muted hover:bg-ink-hover",
                    "border-light-border text-light-text-muted hover:bg-light-hover",
                  )}`}
                >
                  Cancel
                </button>
                <button
                  onClick={handleCreate}
                  disabled={putStore.isPending}
                  className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                >
                  {putStore.isPending ? "Creating..." : "Create"}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Existing stores */}
        {stores.map((store) => {
          const edit = getEdit(store);
          const hasPolicy = store.policy && store.policy in policies;
          const hasFilter = store.filter && store.filter in filters;
          const hasRetention =
            store.retention && store.retention in retentionPolicies;
          const warnings = [
            ...(!hasPolicy ? ["no rotation policy"] : []),
            ...(!hasRetention ? ["no retention policy"] : []),
            ...(!hasFilter ? ["no filter"] : []),
          ];
          return (
            <SettingsCard
              key={store.id}
              id={store.id}
              typeBadge={store.type}
              dark={dark}
              expanded={expanded === store.id}
              onToggle={() =>
                setExpanded(expanded === store.id ? null : store.id)
              }
              onDelete={() => handleDelete(store.id)}
              deleteLabel="Delete"
              status={
                warnings.length > 0 ? (
                  <span className="text-[0.85em] text-severity-warn">
                    {warnings.join(", ")}
                  </span>
                ) : undefined
              }
            >
              <div className="flex flex-col gap-3">
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
                <div className="flex justify-end pt-2">
                  <button
                    onClick={() => handleSave(store.id, store.type)}
                    disabled={putStore.isPending}
                    className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                  >
                    {putStore.isPending ? "Saving..." : "Save"}
                  </button>
                </div>
              </div>
            </SettingsCard>
          );
        })}

        {stores.length === 0 && !adding && (
          <div
            className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            No stores configured. Click "Add Store" to create one.
          </div>
        )}
      </div>
    </div>
  );
}
