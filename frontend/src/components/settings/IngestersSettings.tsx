import { useState, useReducer } from "react";
import { useConfig, usePutIngester, useDeleteIngester, useGenerateName } from "../../api/hooks";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { IngesterParamsForm } from "./IngesterParamsForm";
import { PrimaryButton, GhostButton } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { NodeBadge } from "./NodeBadge";
import { NodeSelect } from "./NodeSelect";

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
  newNodeId: string;
}

const addIngesterFormInitial: AddIngesterFormState = {
  adding: false,
  typeConfirmed: false,
  newName: "",
  newType: "chatterbox",
  newParams: {},
  newNodeId: "",
};

type AddIngesterFormAction =
  | { type: "setAdding"; value: boolean }
  | { type: "confirmType"; ingesterType: string }
  | { type: "setNewName"; value: string }
  | { type: "setNewParams"; value: Record<string, string> }
  | { type: "setNewNodeId"; value: string }
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
    case "setNewNodeId":
      return { ...state, newNodeId: action.value };
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
  const generateName = useGenerateName();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);

  const [addForm, dispatchAdd] = useReducer(addIngesterFormReducer, addIngesterFormInitial);
  const { adding, typeConfirmed, newName, newType, newParams, newNodeId } = addForm;
  const [namePlaceholder, setNamePlaceholder] = useState("");

  const ingesters = config?.ingesters ?? [];
  const existingNames = new Set(ingesters.map((i) => i.name));
  const effectiveName = newName.trim() || namePlaceholder || newType;
  const nameConflict = existingNames.has(effectiveName);

  const defaults = (id: string) => {
    const ing = ingesters.find((i) => i.id === id);
    if (!ing) return { name: "", enabled: true, params: {} as Record<string, string>, nodeId: "" };
    return { name: ing.name, enabled: ing.enabled, params: { ...ing.params }, nodeId: ing.nodeId };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveIngester, handleDelete } = useCrudHandlers({
    mutation: putIngester,
    deleteMutation: deleteIngester,
    label: "Ingester",
    onSaveTransform: (
      id,
      edit: { name: string; enabled: boolean; params: Record<string, string>; type: string; nodeId: string },
    ) => ({
      id,
      name: edit.name,
      type: edit.type,
      enabled: edit.enabled,
      params: edit.params,
      nodeId: edit.nodeId,
    }),
    clearEdit,
  });

  const handleCreate = async () => {
    const name = newName.trim() || namePlaceholder || newType;
    try {
      await putIngester.mutateAsync({
        id: "",
        name,
        type: newType,
        enabled: true,
        params: newParams,
        nodeId: newNodeId,
      });
      addToast(`Ingester "${name}" created`, "info");
      dispatchAdd({ type: "resetForm" });
    } catch (err: any) {
      addToast(err.message ?? "Failed to create ingester", "error");
    }
  };

  return (
    <SettingsSection
      addLabel="Add Ingester"
      adding={adding}
      onToggleAdd={() => {
        if (!adding) {
          generateName.mutateAsync().then(setNamePlaceholder);
        } else {
          setNamePlaceholder("");
        }
        dispatchAdd({ type: "toggleAdding" });
      }}
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
              placeholder={namePlaceholder || newType}
              dark={dark}
            />
          </FormField>
          <NodeSelect
            value={newNodeId}
            onChange={(v) => dispatchAdd({ type: "setNewNodeId", value: v })}
            dark={dark}
          />
          <IngesterParamsForm
            ingesterType={newType}
            params={newParams}
            onChange={(v) => dispatchAdd({ type: "setNewParams", value: v })}
            dark={dark}
          />
        </AddFormCard>
      )}

      {ingesters.toSorted((a, b) => a.name.localeCompare(b.name)).map((ing) => {
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
              <span className="flex items-center gap-2">
                <NodeBadge nodeId={ing.nodeId} dark={dark} />
                {!ing.enabled && (
                  <Badge variant="ghost" dark={dark}>disabled</Badge>
                )}
              </span>
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
              <NodeSelect
                value={edit.nodeId}
                onChange={(v) => setEdit(ing.id, { nodeId: v })}
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
