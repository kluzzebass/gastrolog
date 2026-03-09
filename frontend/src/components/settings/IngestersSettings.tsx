import { useState, useReducer } from "react";
import { useExpandedCard } from "../../hooks/useExpandedCards";
import { useConfig, usePutIngester, useDeleteIngester, useGenerateName } from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput } from "./FormField";
import { IngesterParamsForm, isIngesterParamsValid } from "./ingester-params";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { NodeBadge } from "./NodeBadge";
import { NodeSelect } from "./NodeSelect";
import { sortByName } from "../../lib/sort";

const ingesterTypes = [
  { value: "chatterbox", label: "chatterbox" },
  { value: "docker", label: "docker" },
  { value: "fluentfwd", label: "fluentfwd" },
  { value: "http", label: "http" },
  { value: "kafka", label: "kafka" },
  { value: "mqtt", label: "mqtt" },
  { value: "metrics", label: "metrics" },
  { value: "otlp", label: "otlp" },
  { value: "relp", label: "relp" },
  { value: "self", label: "self" },
  { value: "syslog", label: "syslog" },
  { value: "tail", label: "tail" },
];

// -- Reducer for "Add ingester" form state --

interface AddIngesterFormState {
  adding: boolean;
  newName: string;
  newType: string;
  newParams: Record<string, string>;
  newNodeId: string;
}

const addIngesterFormInitial: AddIngesterFormState = {
  adding: false,
  newName: "",
  newType: "chatterbox",
  newParams: {},
  newNodeId: "",
};

type AddIngesterFormAction =
  | { type: "startAdd"; ingesterType: string }
  | { type: "setNewName"; value: string }
  | { type: "setNewParams"; value: Record<string, string> }
  | { type: "setNewNodeId"; value: string }
  | { type: "resetForm" };

function addIngesterFormReducer(state: AddIngesterFormState, action: AddIngesterFormAction): AddIngesterFormState {
  switch (action.type) {
    case "startAdd":
      return { ...addIngesterFormInitial, adding: true, newType: action.ingesterType };
    case "setNewName":
      return { ...state, newName: action.value };
    case "setNewParams":
      return { ...state, newParams: action.value };
    case "setNewNodeId":
      return { ...state, newNodeId: action.value };
    case "resetForm":
      return addIngesterFormInitial;
    default:
      return state;
  }
}

export function IngestersSettings({ dark }: Readonly<{ dark: boolean }>) {
  const { data: config, isLoading } = useConfig();
  const putIngester = usePutIngester();
  const deleteIngester = useDeleteIngester();
  const generateName = useGenerateName();
  const { addToast } = useToast();

  const { isExpanded, toggle: toggleCard } = useExpandedCard();

  const [addForm, dispatchAdd] = useReducer(addIngesterFormReducer, addIngesterFormInitial);
  const { adding, newName, newType, newParams, newNodeId } = addForm;
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
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create ingester", "error");
    }
  };

  return (
    <SettingsSection
      addLabel="Add Ingester"
      adding={adding}
      onToggleAdd={() => {
        setNamePlaceholder("");
        dispatchAdd({ type: "resetForm" });
      }}
      addOptions={ingesterTypes}
      onAddSelect={(type) => {
        generateName.mutateAsync().then(setNamePlaceholder);
        dispatchAdd({ type: "startAdd", ingesterType: type });
      }}
      isLoading={isLoading}
      isEmpty={ingesters.length === 0}
      emptyMessage='No ingesters configured. Click "Add Ingester" to create one.'
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => dispatchAdd({ type: "resetForm" })}
          onCreate={handleCreate}
          isPending={putIngester.isPending}
          createDisabled={nameConflict || !isIngesterParamsValid(newType, newParams)}
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

      {sortByName(ingesters).map((ing) => {
        const edit = getEdit(ing.id);
        return (
          <SettingsCard
            key={ing.id}
            id={ing.name || ing.id}
            typeBadge={ing.type}
            dark={dark}
            expanded={isExpanded(ing.id)}
            onToggle={() => toggleCard(ing.id)}
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
              <Button
                onClick={() =>
                  saveIngester(ing.id, {
                    ...getEdit(ing.id),
                    type: ing.type,
                  })
                }
                disabled={putIngester.isPending || !isDirty(ing.id) || !isIngesterParamsValid(ing.type, edit.params)}
              >
                {putIngester.isPending ? "Saving..." : "Save"}
              </Button>
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
