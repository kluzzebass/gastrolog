import { useState, useCallback } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useConfig, usePutStore, useDeleteStore } from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { StoreParamsForm } from "./StoreParamsForm";
import { PrimaryButton } from "./Buttons";

export function StoresSettings({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putStore = usePutStore();
  const deleteStore = useDeleteStore();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

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

  const defaults = useCallback(
    (id: string) => {
      const store = stores.find((s) => s.id === id);
      if (!store)
        return {
          filter: "",
          policy: "",
          retention: "",
          params: {} as Record<string, string>,
        };
      return {
        filter: store.filter,
        policy: store.policy,
        retention: store.retention,
        params: { ...store.params },
      };
    },
    [stores],
  );

  const { getEdit, setEdit, clearEdit } = useEditState(defaults);

  const handleSave = async (id: string, type: string) => {
    const edit = getEdit(id);
    try {
      await putStore.mutateAsync({
        id,
        type,
        filter: edit.filter,
        policy: edit.policy,
        retention: edit.retention,
        params: edit.params,
      });
      clearEdit(id);
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
        </AddFormCard>
      )}

      {stores.map((store) => {
        const edit = getEdit(store.id);
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
            footer={
              <PrimaryButton
                onClick={() => handleSave(store.id, store.type)}
                disabled={putStore.isPending}
              >
                {putStore.isPending ? "Saving..." : "Save"}
              </PrimaryButton>
            }
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
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}
