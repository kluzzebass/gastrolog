import { useState, useCallback } from "react";
import { useConfig, usePutIngester, useDeleteIngester } from "../../api/hooks";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput } from "./FormField";
import { IngesterParamsForm } from "./IngesterParamsForm";
import { PrimaryButton, GhostButton } from "./Buttons";
import { Checkbox } from "./Checkbox";

const ingesterTypes = [
  { value: "chatterbox", label: "chatterbox" },
  { value: "docker", label: "docker" },
  { value: "http", label: "http" },
  { value: "relp", label: "relp" },
  { value: "syslog", label: "syslog" },
  { value: "tail", label: "tail" },
];

export function IngestersSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putIngester = usePutIngester();
  const deleteIngester = useDeleteIngester();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [typeConfirmed, setTypeConfirmed] = useState(false);

  const [newName, setNewName] = useState("");
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

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveIngester, handleDelete } = useCrudHandlers({
    mutation: putIngester,
    deleteMutation: deleteIngester,
    label: "Ingester",
    onSaveTransform: (
      id,
      edit: { enabled: boolean; params: Record<string, string>; type: string },
    ) => ({
      id,
      name: ingesters.find((i) => i.id === id)?.name ?? "",
      type: edit.type,
      enabled: edit.enabled,
      params: edit.params,
    }),
    clearEdit,
  });

  const handleCreate = async () => {
    if (!newName.trim()) {
      addToast("Ingester name is required", "warn");
      return;
    }
    try {
      await putIngester.mutateAsync({
        id: "",
        name: newName.trim(),
        type: newType,
        enabled: true,
        params: newParams,
      });
      addToast(`Ingester "${newName.trim()}" created`, "info");
      setAdding(false);
      setTypeConfirmed(false);
      setNewName("");
      setNewType("chatterbox");
      setNewParams({});
    } catch (err: any) {
      addToast(err.message ?? "Failed to create ingester", "error");
    }
  };

  return (
    <SettingsSection
      title="Ingesters"
      helpTopicId="ingesters"
      addLabel="Add Ingester"
      adding={adding}
      onToggleAdd={() => {
        setAdding(!adding);
        setTypeConfirmed(false);
        setNewName("");
        setNewType("chatterbox");
        setNewParams({});
      }}
      isLoading={isLoading}
      isEmpty={ingesters.length === 0}
      emptyMessage='No ingesters configured. Click "Add Ingester" to create one.'
      dark={dark}
      addSlot={
        adding && !typeConfirmed ? (
          <div className="flex items-center gap-1.5 flex-wrap">
            {ingesterTypes.map((t) => (
              <button
                key={t.value}
                type="button"
                onClick={() => {
                  setNewType(t.value);
                  setNewParams({});
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
          isPending={putIngester.isPending}
          typeBadge={newType}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={setNewName}
              placeholder="my-ingester"
              dark={dark}
            />
          </FormField>
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
            id={ing.name || ing.id}
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
                onClick={() =>
                  saveIngester(ing.id, {
                    ...getEdit(ing.id),
                    type: ing.type,
                  })
                }
                disabled={putIngester.isPending || !isDirty(ing.id)}
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
