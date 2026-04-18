import { encode } from "../../api/glid";
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
import { IngesterCommonFields } from "./IngesterCommonFields";
import { IngesterParamsForm, isIngesterParamsValid, listenAddrConflict } from "./ingester-params";
import { Button } from "./Buttons";
import { NodeBadge } from "./NodeBadge";
import { sortByName } from "../../lib/sort";
import { PulseIcon } from "../icons";
import { CrossLinkBadge } from "../inspector/CrossLinkBadge";
import { IngesterMode, type IngesterConfig } from "../../api/gen/gastrolog/v1/system_pb";
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
  newEnabled: boolean;
  newParams: Record<string, string>;
  newNodeIds: string[];
  newSingleton: boolean;
}

const addIngesterFormInitial: AddIngesterFormState = {
  adding: false,
  newName: "",
  newType: "chatterbox",
  newEnabled: true,
  newParams: {},
  newNodeIds: [],
  newSingleton: false,
};

type AddIngesterFormAction =
  | { type: "startAdd"; ingesterType: string }
  | { type: "setNewName"; value: string }
  | { type: "setNewEnabled"; value: boolean }
  | { type: "setNewParams"; value: Record<string, string> }
  | { type: "setNewNodeIds"; value: string[] }
  | { type: "setNewSingleton"; value: boolean }
  | { type: "resetForm" };

function addIngesterFormReducer(state: AddIngesterFormState, action: AddIngesterFormAction): AddIngesterFormState {
  switch (action.type) {
    case "startAdd":
      return { ...addIngesterFormInitial, adding: true, newType: action.ingesterType };
    case "setNewName":
      return { ...state, newName: action.value };
    case "setNewEnabled":
      return { ...state, newEnabled: action.value };
    case "setNewParams":
      return { ...state, newParams: action.value };
    case "setNewNodeIds":
      return { ...state, newNodeIds: action.value };
    case "setNewSingleton":
      return { ...state, newSingleton: action.value };
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
  const { data: ingesterMeta } = useIngesterDefaults();
  const allDefaults = ingesterMeta?.defaults ?? {};
  const ingesterModes = ingesterMeta?.modes ?? {};
  const singletonSupport = ingesterMeta?.singletonSupported ?? {};
  const { addToast } = useToast();

  const { isExpanded, toggle: toggleCard, setExpandedCards } = useExpandedCards();

  const [addForm, dispatchAdd] = useReducer(addIngesterFormReducer, addIngesterFormInitial);
  const { adding, newName, newType, newEnabled, newParams, newNodeIds, newSingleton } = addForm;
  const [namePlaceholder, setNamePlaceholder] = useState("");

  const configIngesters = config?.ingesters;
  const ingesters = configIngesters ?? [];

  // Auto-expand an ingester when navigated to from another view.
  const [consumedExpandTarget, setConsumedExpandTarget] = useState<string | null>(null);
  if (expandTarget && expandTarget !== consumedExpandTarget && configIngesters && configIngesters.length > 0) {
    setConsumedExpandTarget(expandTarget);
    const match = configIngesters.find((i) => (i.name || encode(i.id)) === expandTarget);
    if (match) {
      setExpandedCards((prev) => ({ ...prev, [encode(match.id)]: true }));
    }
    onExpandTargetConsumed?.();
  }
  const existingNames = new Set(ingesters.map((i) => i.name));
  const effectiveName = newName.trim() || namePlaceholder || newType;
  const nameConflict = existingNames.has(effectiveName);
  const newAddrConflict = listenAddrConflict("", newType, newParams, newNodeIds, ingesters, allDefaults);
  const newPortCheck = useCheckListenAddrs(newType, newParams, "");
  const newPortError = !newAddrConflict && newPortCheck.data && !newPortCheck.data.success ? newPortCheck.data.message : null;
  const newListenError = newAddrConflict ?? newPortError;

  const defaults = (id: string) => {
    const ing = ingesters.find((i) => encode(i.id) === id);
    if (!ing) return { name: "", enabled: true, params: {} as Record<string, string>, nodeIds: [] as string[], singleton: false };
    return { name: ing.name, enabled: ing.enabled, params: { ...ing.params }, nodeIds: ing.nodeIds.map(encode), singleton: ing.singleton };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveIngester, handleDelete } = useCrudHandlers({
    mutation: putIngester,
    deleteMutation: deleteIngester,
    label: "Ingester",
    onSaveTransform: (
      id,
      edit: { name: string; enabled: boolean; params: Record<string, string>; type: string; nodeIds: string[]; singleton: boolean },
    ) => ({
      id,
      name: edit.name,
      type: edit.type,
      enabled: edit.enabled,
      params: edit.params,
      nodeIds: edit.nodeIds,
      singleton: edit.singleton,
    }),
    clearEdit,
  });

  const handleCreate = async () => {
    const name = newName.trim() || namePlaceholder || newType;
    try {
      await putIngester.mutateAsync({
        id: encode(crypto.getRandomValues(new Uint8Array(16))),
        name,
        type: newType,
        enabled: newEnabled,
        params: newParams,
        nodeIds: newNodeIds,
        singleton: newSingleton,
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
        generateName.mutateAsync().then(setNamePlaceholder).catch(() => {});
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
          <IngesterCommonFields
            name={newName}
            namePlaceholder={namePlaceholder || newType}
            onNameChange={(v) => dispatchAdd({ type: "setNewName", value: v })}
            enabled={newEnabled}
            onEnabledChange={(v) => dispatchAdd({ type: "setNewEnabled", value: v })}
            nodeIds={newNodeIds}
            onNodeIdsChange={(v) => dispatchAdd({ type: "setNewNodeIds", value: v })}
            singleton={newSingleton}
            onSingletonChange={(v) => dispatchAdd({ type: "setNewSingleton", value: v })}
            singletonSupported={singletonSupport[newType] ?? false}
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
          key={encode(ing.id)}
          ing={ing}
          allIngesters={ingesters}
          allDefaults={allDefaults}
          mode={ingesterModes[ing.type] ?? IngesterMode.ACTIVE}
          singletonSupported={singletonSupport[ing.type] ?? false}
          dark={dark}
          expanded={isExpanded(encode(ing.id))}
          onToggle={() => toggleCard(encode(ing.id))}
          onDelete={() => handleDelete(encode(ing.id))}
          onSave={(id) => saveIngester(id, { ...getEdit(id), type: ing.type })}
          onDiscard={() => clearEdit(encode(ing.id))}
          isSaving={putIngester.isPending}
          edit={getEdit(encode(ing.id))}
          setEdit={(patch) => setEdit(encode(ing.id), patch)}
          isDirty={isDirty(encode(ing.id))}
          onOpenInspector={onOpenInspector}
        />
      ))}
    </SettingsSection>
  );
}

function IngesterNodeBadge({ nodeIds, mode, dark }: Readonly<{ nodeIds: Uint8Array[]; mode: IngesterMode; dark: boolean }>) {
  if (mode === IngesterMode.PASSIVE) return <Badge variant="info" dark={dark}>all nodes</Badge>;
  if (nodeIds.length > 1) return <Badge variant="muted" dark={dark}>{String(nodeIds.length)} nodes</Badge>;
  if (nodeIds.length === 1) return <NodeBadge nodeId={encode(nodeIds[0]!)} dark={dark} />; // NOSONAR — length check guards access
  return null;
}

function IngesterCard({
  ing,
  allIngesters,
  allDefaults,
  mode,
  singletonSupported,
  dark,
  expanded,
  onToggle,
  onDelete,
  onSave,
  onDiscard,
  isSaving,
  edit,
  setEdit,
  isDirty,
  onOpenInspector,
}: Readonly<{
  ing: IngesterConfig;
  allIngesters: readonly IngesterConfig[];
  allDefaults: IngesterDefaults;
  mode: IngesterMode;
  singletonSupported: boolean;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onDelete: () => void;
  onSave: (id: string) => void;
  onDiscard: () => void;
  isSaving: boolean;
  edit: { name: string; enabled: boolean; params: Record<string, string>; nodeIds: string[]; singleton: boolean };
  setEdit: (patch: Partial<{ name: string; enabled: boolean; params: Record<string, string>; nodeIds: string[]; singleton: boolean }>) => void;
  isDirty: boolean;
  onOpenInspector?: (inspectorParam: string) => void;
}>) {
  const addrConflict = listenAddrConflict(encode(ing.id), ing.type, edit.params, edit.nodeIds, allIngesters, allDefaults);
  const portCheck = useCheckListenAddrs(ing.type, edit.params, encode(ing.id));
  const portError = !addrConflict && portCheck.data && !portCheck.data.success ? portCheck.data.message : null;
  const listenError = addrConflict ?? portError;

  return (
    <SettingsCard
      id={ing.name || encode(ing.id)}
      typeBadge={ing.type}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={onDelete}
      headerRight={
        <span className="flex items-center gap-2">
          <Badge variant="muted" dark={dark}>{mode === IngesterMode.PASSIVE ? "listener" : "collector"}</Badge>
          <IngesterNodeBadge nodeIds={ing.nodeIds} mode={mode} dark={dark} />
          {!ing.enabled && (
            <Badge variant="muted" dark={dark}>disabled</Badge>
          )}
          {onOpenInspector && (
            <CrossLinkBadge dark={dark} title="Open in Inspector" onClick={() => onOpenInspector(`entities:ingesters:${ing.name || encode(ing.id)}`)}>
              <PulseIcon className="w-3 h-3" />
            </CrossLinkBadge>
          )}
        </span>
      }
      footer={
        <>
          {isDirty && (
            <Button onClick={onDiscard} disabled={isSaving} dark={dark} variant="ghost">
              Discard
            </Button>
          )}
          <Button
            onClick={() => onSave(encode(ing.id))}
            disabled={isSaving || !isDirty || !!listenError || !isIngesterParamsValid(ing.type, edit.params)}
          >
            {isSaving ? "Saving..." : "Save"}
          </Button>
        </>
      }
    >
      <div className="flex flex-col gap-3">
        <IngesterCommonFields
          name={edit.name}
          onNameChange={(v) => setEdit({ name: v })}
          enabled={edit.enabled}
          onEnabledChange={(v) => setEdit({ enabled: v })}
          nodeIds={edit.nodeIds}
          onNodeIdsChange={(v) => setEdit({ nodeIds: v })}
          singleton={edit.singleton}
          onSingletonChange={(v) => setEdit({ singleton: v })}
          singletonSupported={singletonSupported}
          dark={dark}
        />
        <IngesterParamsForm
          ingesterType={ing.type}
          params={edit.params}
          onChange={(p) => setEdit({ params: p })}
          dark={dark}
          ingesterId={encode(ing.id)}
          ingesterNodeId={edit.nodeIds[0] ?? ""}
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
