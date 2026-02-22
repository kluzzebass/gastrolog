import { useState, useReducer } from "react";
import { useConfig, usePutIngester, useDeleteIngester } from "../../api/hooks";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { IngesterParamsForm } from "./IngesterParamsForm";
import { PrimaryButton, GhostButton } from "./Buttons";
import { Checkbox } from "./Checkbox";

const ingesterTypes = [
  { value: "chatterbox", label: "chatterbox" },
  { value: "docker", label: "docker" },
  { value: "fluentfwd", label: "fluentfwd" },
  { value: "http", label: "http" },
  { value: "kafka", label: "kafka" },
  { value: "metrics", label: "metrics" },
  { value: "otlp", label: "otlp" },
  { value: "relp", label: "relp" },
  { value: "syslog", label: "syslog" },
  { value: "tail", label: "tail" },
];

// -- Reducer for "Add ingester" form state --

interface AddIngesterFormState {
  adding: boolean;
  typeConfirmed: boolean;
  newName: string;
  newType: string;
  newParams: Record<string, string>;
}

const addIngesterFormInitial: AddIngesterFormState = {
  adding: false,
  typeConfirmed: false,
  newName: "",
  newType: "chatterbox",
  newParams: {},
};

type AddIngesterFormAction =
  | { type: "setAdding"; value: boolean }
  | { type: "confirmType"; ingesterType: string }
  | { type: "setNewName"; value: string }
  | { type: "setNewParams"; value: Record<string, string> }
  | { type: "resetForm" }
  | { type: "toggleAdding" };

function addIngesterFormReducer(state: AddIngesterFormState, action: AddIngesterFormAction): AddIngesterFormState {
  switch (action.type) {
    case "setAdding":
      return { ...state, adding: action.value };
    case "confirmType":
      return { ...state, newType: action.ingesterType, newParams: {}, typeConfirmed: true };
    case "setNewName":
      return { ...state, newName: action.value };
    case "setNewParams":
      return { ...state, newParams: action.value };
    case "resetForm":
      return addIngesterFormInitial;
    case "toggleAdding":
      return state.adding
        ? addIngesterFormInitial
        : { ...addIngesterFormInitial, adding: true };
    default:
      return state;
  }
}

export function IngestersSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putIngester = usePutIngester();
  const deleteIngester = useDeleteIngester();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);

  const [addForm, dispatchAdd] = useReducer(addIngesterFormReducer, addIngesterFormInitial);
  const { adding, typeConfirmed, newName, newType, newParams } = addForm;

  const ingesters = config?.ingesters ?? [];
  const existingNames = new Set(ingesters.map((i) => i.name));
  const effectiveName = newName.trim() || newType;
  const nameConflict = existingNames.has(effectiveName);

  const defaults = (id: string) => {
    const ing = ingesters.find((i) => i.id === id);
    if (!ing) return { name: "", enabled: true, params: {} as Record<string, string> };
    return { name: ing.name, enabled: ing.enabled, params: { ...ing.params } };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveIngester, handleDelete } = useCrudHandlers({
    mutation: putIngester,
    deleteMutation: deleteIngester,
    label: "Ingester",
    onSaveTransform: (
      id,
      edit: { name: string; enabled: boolean; params: Record<string, string>; type: string },
    ) => ({
      id,
      name: edit.name,
      type: edit.type,
      enabled: edit.enabled,
      params: edit.params,
    }),
    clearEdit,
  });

  const handleCreate = async () => {
    const name = newName.trim() || newType;
    try {
      await putIngester.mutateAsync({
        id: "",
        name,
        type: newType,
        enabled: true,
        params: newParams,
      });
      addToast(`Ingester "${name}" created`, "info");
      dispatchAdd({ type: "resetForm" });
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
      onToggleAdd={() => dispatchAdd({ type: "toggleAdding" })}
      isLoading={isLoading}
      isEmpty={ingesters.length === 0}
      emptyMessage='No ingesters configured. Click "Add Ingester" to create one.'
      dark={dark}
      addSlot={
        adding && !typeConfirmed ? (
          <div className="flex items-center gap-1.5">
            <SelectInput
              value=""
              onChange={(v) => {
                if (v) dispatchAdd({ type: "confirmType", ingesterType: v });
              }}
              options={[
                { value: "", label: "Select type\u2026" },
                ...ingesterTypes,
              ]}
              dark={dark}
            />
            <GhostButton
              onClick={() => dispatchAdd({ type: "resetForm" })}
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
          onCancel={() => dispatchAdd({ type: "resetForm" })}
          onCreate={handleCreate}
          isPending={putIngester.isPending}
          createDisabled={nameConflict}
          typeBadge={newType}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={(v) => dispatchAdd({ type: "setNewName", value: v })}
              placeholder={newType}
              dark={dark}
            />
          </FormField>
          <IngesterParamsForm
            ingesterType={newType}
            params={newParams}
            onChange={(v) => dispatchAdd({ type: "setNewParams", value: v })}
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
              <FormField label="Name" dark={dark}>
                <TextInput
                  value={edit.name}
                  onChange={(v) => setEdit(ing.id, { name: v })}
                  dark={dark}
                />
              </FormField>
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
