import { useState, useReducer } from "react";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { useConfig, usePutIngester, useDeleteIngester, useGenerateName, useIngesterDefaults, useCheckListenAddrs } from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput } from "./FormField";
import { IngesterParamsForm, isIngesterParamsValid, listenAddrConflict } from "./ingester-params";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { NodeBadge } from "./NodeBadge";
import { NodeSelect } from "./NodeSelect";
import { sortByName } from "../../lib/sort";
import { PulseIcon } from "../icons";
import { CrossLinkBadge } from "../inspector/CrossLinkBadge";
import type { IngesterConfig } from "../../api/gen/gastrolog/v1/config_pb";
import type { IngesterDefaults } from "../../api/hooks/useIngesterDefaults";

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
  { value: "scatterbox", label: "scatterbox" },
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

export function IngestersSettings({ dark, expandTarget, onExpandTargetConsumed, onOpenInspector }: Readonly<{ dark: boolean; expandTarget?: string | null; onExpandTargetConsumed?: () => void; onOpenInspector?: (inspectorParam: string) => void }>) {
  const { data: config, isLoading } = useConfig();
  const putIngester = usePutIngester();
  const deleteIngester = useDeleteIngester();
  const generateName = useGenerateName();
  const { data: ingesterDefaults } = useIngesterDefaults();
  const allDefaults = ingesterDefaults ?? {};
  const { addToast } = useToast();

  const { isExpanded, toggle: toggleCard, setExpandedCards } = useExpandedCards();

  const [addForm, dispatchAdd] = useReducer(addIngesterFormReducer, addIngesterFormInitial);
  const { adding, newName, newType, newParams, newNodeId } = addForm;
  const [namePlaceholder, setNamePlaceholder] = useState("");

  const configIngesters = config?.ingesters;
  const ingesters = configIngesters ?? [];

  // Auto-expand an ingester when navigated to from another view.
  const [consumedExpandTarget, setConsumedExpandTarget] = useState<string | null>(null);
  if (expandTarget && expandTarget !== consumedExpandTarget && configIngesters && configIngesters.length > 0) {
    setConsumedExpandTarget(expandTarget);
    const match = configIngesters.find((i) => (i.name || i.id) === expandTarget);
    if (match) {
      setExpandedCards((prev) => ({ ...prev, [match.id]: true }));
    }
    onExpandTargetConsumed?.();
  }
  const existingNames = new Set(ingesters.map((i) => i.name));
  const effectiveName = newName.trim() || namePlaceholder || newType;
  const nameConflict = existingNames.has(effectiveName);
  const newAddrConflict = listenAddrConflict("", newType, newParams, newNodeId, ingesters, allDefaults);
  const newPortCheck = useCheckListenAddrs(newType, newParams, "");
  const newPortError = !newAddrConflict && newPortCheck.data && !newPortCheck.data.success ? newPortCheck.data.message : null;
  const newListenError = newAddrConflict ?? newPortError;

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
          createDisabled={nameConflict || !!newListenError || !isIngesterParamsValid(newType, newParams)}
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
          {newListenError && (
            <p className={`text-[0.8em] text-severity-error`}>
              {newListenError}
            </p>
          )}
        </AddFormCard>
      )}

      {sortByName(ingesters).map((ing) => (
        <IngesterCard
          key={ing.id}
          ing={ing}
          allIngesters={ingesters}
          allDefaults={allDefaults}
          dark={dark}
          expanded={isExpanded(ing.id)}
          onToggle={() => toggleCard(ing.id)}
          onDelete={() => handleDelete(ing.id)}
          onSave={(id) => saveIngester(id, { ...getEdit(id), type: ing.type })}
          isSaving={putIngester.isPending}
          edit={getEdit(ing.id)}
          setEdit={(patch) => setEdit(ing.id, patch)}
          isDirty={isDirty(ing.id)}
          onOpenInspector={onOpenInspector}
        />
      ))}
    </SettingsSection>
  );
}

function IngesterCard({
  ing,
  allIngesters,
  allDefaults,
  dark,
  expanded,
  onToggle,
  onDelete,
  onSave,
  isSaving,
  edit,
  setEdit,
  isDirty,
  onOpenInspector,
}: Readonly<{
  ing: IngesterConfig;
  allIngesters: readonly IngesterConfig[];
  allDefaults: IngesterDefaults;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onDelete: () => void;
  onSave: (id: string) => void;
  isSaving: boolean;
  edit: { name: string; enabled: boolean; params: Record<string, string>; nodeId: string };
  setEdit: (patch: Partial<{ name: string; enabled: boolean; params: Record<string, string>; nodeId: string }>) => void;
  isDirty: boolean;
  onOpenInspector?: (inspectorParam: string) => void;
}>) {
  const addrConflict = listenAddrConflict(ing.id, ing.type, edit.params, edit.nodeId, allIngesters, allDefaults);
  const portCheck = useCheckListenAddrs(ing.type, edit.params, ing.id);
  const portError = !addrConflict && portCheck.data && !portCheck.data.success ? portCheck.data.message : null;
  const listenError = addrConflict ?? portError;

  return (
    <SettingsCard
      id={ing.name || ing.id}
      typeBadge={ing.type}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={onDelete}
      headerRight={
        <span className="flex items-center gap-2">
          <NodeBadge nodeId={ing.nodeId} dark={dark} />
          {!ing.enabled && (
            <Badge variant="ghost" dark={dark}>disabled</Badge>
          )}
          {onOpenInspector && (
            <CrossLinkBadge dark={dark} title="Open in Inspector" onClick={() => onOpenInspector(`entities:ingesters:${ing.name || ing.id}`)}>
              <PulseIcon className="w-3 h-3" />
            </CrossLinkBadge>
          )}
        </span>
      }
      footer={
        <Button
          onClick={() => onSave(ing.id)}
          disabled={isSaving || !isDirty || !!listenError || !isIngesterParamsValid(ing.type, edit.params)}
        >
          {isSaving ? "Saving..." : "Save"}
        </Button>
      }
    >
      <div className="flex flex-col gap-3">
        <FormField label="Name" dark={dark}>
          <TextInput
            value={edit.name}
            onChange={(v) => setEdit({ name: v })}
            dark={dark}
          />
        </FormField>
        <Checkbox
          checked={edit.enabled}
          onChange={(v) => setEdit({ enabled: v })}
          label="Enabled"
          dark={dark}
        />
        <NodeSelect
          value={edit.nodeId}
          onChange={(v) => setEdit({ nodeId: v })}
          dark={dark}
        />
        <IngesterParamsForm
          ingesterType={ing.type}
          params={edit.params}
          onChange={(p) => setEdit({ params: p })}
          dark={dark}
          ingesterId={ing.id}
        />
        {listenError && (
          <p className={`text-[0.8em] text-severity-error`}>
            {listenError}
          </p>
        )}
      </div>
    </SettingsCard>
  );
}
