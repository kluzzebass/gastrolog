import { useState, useCallback } from "react";
import { useConfig, usePutIngester, useDeleteIngester } from "../../api/hooks";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { IngesterParamsForm } from "./IngesterParamsForm";
import { PrimaryButton } from "./Buttons";
import { Checkbox } from "./Checkbox";

const ingesterTypes = [
  { value: "chatterbox", label: "chatterbox" },
  { value: "http", label: "http" },
  { value: "relp", label: "relp" },
  { value: "syslog", label: "syslog" },
  { value: "tail", label: "tail" },
];

export function IngestersSettings({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putIngester = usePutIngester();
  const deleteIngester = useDeleteIngester();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  const [newId, setNewId] = useState("");
  const [newType, setNewType] = useState("chatterbox");
  const [newParams, setNewParams] = useState<Record<string, string>>({});

  const ingesters = config?.ingesters ?? [];

  const defaults = useCallback(
    (id: string) => {
      const ing = ingesters.find((i) => i.id === id);
      if (!ing) return { enabled: true, params: {} as Record<string, string> };
      return { enabled: ing.enabled, params: { ...ing.params } };
    },
    [ingesters],
  );

  const { getEdit, setEdit, clearEdit } = useEditState(defaults);

  const handleSave = async (id: string, type: string) => {
    const edit = getEdit(id);
    try {
      await putIngester.mutateAsync({
        id,
        type,
        enabled: edit.enabled,
        params: edit.params,
      });
      clearEdit(id);
      addToast(`Ingester "${id}" updated`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update ingester", "error");
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteIngester.mutateAsync(id);
      addToast(`Ingester "${id}" deleted`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to delete ingester", "error");
    }
  };

  const handleNewTypeChange = (type: string) => {
    setNewType(type);
    setNewParams({});
  };

  const handleCreate = async () => {
    if (!newId.trim()) {
      addToast("Ingester ID is required", "warn");
      return;
    }
    try {
      await putIngester.mutateAsync({
        id: newId.trim(),
        type: newType,
        enabled: true,
        params: newParams,
      });
      addToast(`Ingester "${newId.trim()}" created`, "info");
      setAdding(false);
      setNewId("");
      setNewType("chatterbox");
      setNewParams({});
    } catch (err: any) {
      addToast(err.message ?? "Failed to create ingester", "error");
    }
  };

  return (
    <SettingsSection
      title="Ingesters"
      addLabel="Add Ingester"
      adding={adding}
      onToggleAdd={() => setAdding(!adding)}
      isLoading={isLoading}
      isEmpty={ingesters.length === 0}
      emptyMessage='No ingesters configured. Click "Add Ingester" to create one.'
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => setAdding(false)}
          onCreate={handleCreate}
          isPending={putIngester.isPending}
        >
          <div className="grid grid-cols-2 gap-3">
            <FormField label="ID" dark={dark}>
              <TextInput
                value={newId}
                onChange={setNewId}
                placeholder="my-ingester"
                dark={dark}
                mono
              />
            </FormField>
            <FormField label="Type" dark={dark}>
              <SelectInput
                value={newType}
                onChange={handleNewTypeChange}
                options={ingesterTypes}
                dark={dark}
              />
            </FormField>
          </div>
          <IngesterParamsForm
            ingesterType={newType}
            params={newParams}
            onChange={setNewParams}
            dark={dark}
          />
        </AddFormCard>
      )}

      {ingesters.map((ing) => {
        const edit = getEdit(ing.id);
        return (
          <SettingsCard
            key={ing.id}
            id={ing.id}
            typeBadge={ing.type}
            dark={dark}
            expanded={expanded === ing.id}
            onToggle={() => setExpanded(expanded === ing.id ? null : ing.id)}
            onDelete={() => handleDelete(ing.id)}
            headerRight={
              !ing.enabled ? (
                <span
                  className={`px-1.5 py-0.5 text-[0.8em] font-mono rounded ${c(
                    "bg-ink-hover text-text-ghost",
                    "bg-light-hover text-light-text-ghost",
                  )}`}
                >
                  disabled
                </span>
              ) : undefined
            }
            footer={
              <PrimaryButton
                onClick={() => handleSave(ing.id, ing.type)}
                disabled={putIngester.isPending}
              >
                {putIngester.isPending ? "Saving..." : "Save"}
              </PrimaryButton>
            }
          >
            <div className="flex flex-col gap-3">
              <Checkbox
                checked={edit.enabled}
                onChange={(v) => setEdit(ing.id, { enabled: v })}
                label="Enabled"
                dark={dark}
              />
              <IngesterParamsForm
                ingesterType={ing.type}
                params={edit.params}
                onChange={(p) => setEdit(ing.id, { params: p })}
                dark={dark}
              />
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}
