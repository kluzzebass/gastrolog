import { encode } from "../../api/glid";
import { useState, useReducer } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import {
  useConfig,
  usePutRetentionPolicy,
  useDeleteRetentionPolicy,
  useGenerateName,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, NumberInput } from "./FormField";
import { Checkbox } from "./Checkbox";
import { Button } from "./Buttons";
import { UsedByStatus, vaultRefsForRetentionPolicy } from "./UsedByStatus";
import type { SettingsTab } from "./SettingsDialog";
import { sortByName } from "../../lib/sort";

type NavigateTo = (tab: SettingsTab, entityName?: string) => void;

// refuseDescription doubles as the checkbox label in both forms — the
// checkbox IS the field, no FormField wrapper.
const refuseDescription = "Refuse new records while a bound is violated.";

const noConditionReason = "Set at least one condition";

/** What deleting a retention policy does to the vaults that use it. */
function retentionPolicyDeleteConsequences(refs: string[]): string[] {
  if (refs.length === 0) return [];
  return [`${refs.join(", ")}: retention policy cleared — nothing expires; only the default floor remains, which refuses but never destroys`];
}

interface PolicyEdit {
  name: string;
  maxAge: string;
  maxSize: string;
  maxChunks: string;
  refuse: boolean;
}

// -- Reducer for "Add retention policy" form state --

interface AddRetentionFormState {
  adding: boolean;
  newName: string;
  newMaxAge: string;
  newMaxSize: string;
  newMaxChunks: string;
  newRefuse: boolean;
}

const addRetentionFormInitial: AddRetentionFormState = {
  adding: false,
  newName: "",
  newMaxAge: "",
  newMaxSize: "",
  newMaxChunks: "",
  // RetentionPolicyConfig.Refuse defaults off (nil reads as false — bounds
  // are drain-first, refusal is the explicit hard mode) — match that here
  // so the add form starts in the same state a freshly-created policy
  // would resolve to.
  newRefuse: false,
};

type AddRetentionFormAction =
  | { type: "setAdding"; value: boolean }
  | { type: "setNewName"; value: string }
  | { type: "setNewMaxAge"; value: string }
  | { type: "setNewMaxSize"; value: string }
  | { type: "setNewMaxChunks"; value: string }
  | { type: "setNewRefuse"; value: boolean }
  | { type: "resetForm" };

function addRetentionFormReducer(state: AddRetentionFormState, action: AddRetentionFormAction): AddRetentionFormState {
  switch (action.type) {
    case "setAdding":
      return { ...state, adding: action.value };
    case "setNewName":
      return { ...state, newName: action.value };
    case "setNewMaxAge":
      return { ...state, newMaxAge: action.value };
    case "setNewMaxSize":
      return { ...state, newMaxSize: action.value };
    case "setNewMaxChunks":
      return { ...state, newMaxChunks: action.value };
    case "setNewRefuse":
      return { ...state, newRefuse: action.value };
    case "resetForm":
      return addRetentionFormInitial;
    default:
      return state;
  }
}

export function RetentionPoliciesSettings({ dark, onNavigateTo }: Readonly<{ dark: boolean; onNavigateTo?: NavigateTo }>) {
  const _c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putPolicy = usePutRetentionPolicy();
  const deletePolicy = useDeleteRetentionPolicy();
  const generateName = useGenerateName();
  const { addToast } = useToast();

  const { isExpanded, toggle: toggleCard } = useExpandedCards();

  const [addForm, dispatchAdd] = useReducer(addRetentionFormReducer, addRetentionFormInitial);
  const { adding, newName, newMaxAge, newMaxSize, newMaxChunks, newRefuse } = addForm;
  const [namePlaceholder, setNamePlaceholder] = useState("");

  const policies = config?.retentionPolicies ?? [];
  const existingNames = new Set(policies.map((p) => p.name));
  const effectiveName = newName.trim() || namePlaceholder || "default";
  const nameConflict = existingNames.has(effectiveName);
  const vaults = config?.vaults ?? [];
  // At least one condition must be set, otherwise the retention policy is
  // a silent no-op. Backend rejects empty policies with InvalidArgument;
  // mirror the rule client-side.
  const newPolicyEmpty = !newMaxAge.trim() && !newMaxSize.trim() && !newMaxChunks.trim();
  let createDisabledReason: string | undefined;
  if (nameConflict) createDisabledReason = `A retention policy named "${effectiveName}" already exists`;
  else if (newPolicyEmpty) createDisabledReason = noConditionReason;

  const defaults = (id: string): PolicyEdit => {
    const pol = policies.find((p) => encode(p.id) === id);
    if (!pol) return { name: "", maxAge: "", maxSize: "", maxChunks: "", refuse: false };
    return {
      name: pol.name,
      maxAge: pol.maxAge,
      maxSize: pol.maxSize,
      maxChunks: pol.maxChunks > BigInt(0) ? pol.maxChunks.toString() : "",
      // proto refuse is optional bool — unset reads as false, matching
      // RetentionPolicyConfig.RefuseEnabled() on the backend.
      refuse: pol.refuse ?? false,
    };
  };

  const { getEdit, setEdit, isDirty } = useEditState(defaults);

  const { handleSave: savePolicy, handleDelete } = useCrudHandlers({
    mutation: putPolicy,
    deleteMutation: deletePolicy,
    label: "Retention policy",
    onSaveTransform: (id, edit: PolicyEdit) => {
      const maxChunksValue = edit.maxChunks ? BigInt(edit.maxChunks) : BigInt(0);
      return {
        id,
        name: edit.name,
        maxAge: edit.maxAge,
        maxSize: edit.maxSize,
        maxChunks: maxChunksValue,
        refuse: edit.refuse,
      };
    },
    onDeleteSuccess: (id) => {
      const referencedBy = vaults
        .filter((v) => v.retentionRules.some((r) => encode(r.retentionPolicyId) === id))
        .map((v) => v.name || encode(v.id));
      if (referencedBy.length > 0) {
        addToast(
          `Retention policy "${id}" deleted (was used by vaults: ${referencedBy.join(", ")})`,
          "warn",
        );
      } else {
        addToast(`Retention policy "${id}" deleted`, "info");
      }
    },
  });

  const handleSave = (id: string) => savePolicy(id, getEdit(id));

  const handleCreate = async () => {
    const name = newName.trim() || namePlaceholder || "default";
    const maxChunksValue = newMaxChunks ? BigInt(newMaxChunks) : BigInt(0);
    try {
      await putPolicy.mutateAsync({
        id: encode(crypto.getRandomValues(new Uint8Array(16))),
        name,
        maxAge: newMaxAge,
        maxSize: newMaxSize,
        maxChunks: maxChunksValue,
        refuse: newRefuse,
      });
      addToast(`Retention policy "${name}" created`, "info");
      dispatchAdd({ type: "resetForm" });
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create retention policy", "error");
    }
  };


  return (
    <SettingsSection
      addLabel="Add Policy"
      adding={adding}
      onToggleAdd={() => {
        if (!adding) {
          generateName.mutateAsync().then(setNamePlaceholder).catch(() => {});
        } else {
          setNamePlaceholder("");
        }
        dispatchAdd({ type: "setAdding", value: !adding });
      }}
      isLoading={isLoading}
      isEmpty={policies.length === 0}
      emptyMessage='No retention policies configured. Click "Add Policy" to create one.'
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => dispatchAdd({ type: "resetForm" })}
          onCreate={handleCreate}
          isPending={putPolicy.isPending}
          createDisabled={nameConflict || newPolicyEmpty}
          createDisabledReason={createDisabledReason}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={(v) => dispatchAdd({ type: "setNewName", value: v })}
              placeholder={namePlaceholder || "default"}
              dark={dark}
            />
          </FormField>
          <div className="grid grid-cols-3 gap-3">
            <FormField
              label="Max Age"
              dark={dark}
            >
              <TextInput
                value={newMaxAge}
                onChange={(v) => dispatchAdd({ type: "setNewMaxAge", value: v })}
                placeholder=""
                dark={dark}
                mono
                examples={["720h", "30d", "90d"]}
              />
            </FormField>
            <FormField
              label="Max Size"
              dark={dark}
              description="The vault's disk-claim bound: oldest chunks drain past it; with Refuse on, new records refuse while over it."
              helpRef="policy-retention#max-size"
            >
              <TextInput
                value={newMaxSize}
                onChange={(v) => dispatchAdd({ type: "setNewMaxSize", value: v })}
                placeholder=""
                dark={dark}
                mono
                examples={["1GB", "10GB", "100GB"]}
              />
            </FormField>
            <FormField label="Max Chunks" dark={dark}>
              <NumberInput
                value={newMaxChunks}
                onChange={(v) => dispatchAdd({ type: "setNewMaxChunks", value: v })}
                placeholder=""
                dark={dark}
              />
            </FormField>
          </div>
          <Checkbox
            checked={newRefuse}
            onChange={(v) => dispatchAdd({ type: "setNewRefuse", value: v })}
            label={refuseDescription}
            helpRef="policy-retention#hard-vs-soft-bounds"
            dark={dark}
          />
        </AddFormCard>
      )}

      {sortByName(policies).map((pol) => {
        const id = encode(pol.id);
        const edit = getEdit(id);
        const refs = vaultRefsForRetentionPolicy(id, vaults);
        // Backend rejects empty policies; disable Save preemptively if the
        // operator has cleared every condition.
        const editEmpty = !edit.maxAge.trim() && !edit.maxSize.trim() && !edit.maxChunks.trim();
        return (
          <SettingsCard
            key={id}
            id={pol.name || id}
            dark={dark}
            expanded={isExpanded(id)}
            onToggle={() => toggleCard(id)}
            onDelete={() => handleDelete(id)}
            deleteConsequences={retentionPolicyDeleteConsequences(refs)}
            footer={
              <Button
                onClick={() => handleSave(id)}
                disabled={putPolicy.isPending || !isDirty(id) || editEmpty}
                disabledReason={editEmpty ? noConditionReason : undefined}
              >
                {putPolicy.isPending ? "Saving..." : "Save"}
              </Button>
            }
            status={<UsedByStatus dark={dark} refs={refs} onNavigate={onNavigateTo && ((ref) => onNavigateTo("vaults", ref))} />}
          >
            <div className="flex flex-col gap-3">
              <FormField label="Name" dark={dark}>
                <TextInput
                  value={edit.name}
                  onChange={(v) => setEdit(id, { name: v })}
                  dark={dark}
                />
              </FormField>
              <div className="grid grid-cols-3 gap-3">
                <FormField
                  label="Max Age"
                      dark={dark}
                >
                  <TextInput
                    value={edit.maxAge}
                    onChange={(v) => setEdit(id, { maxAge: v })}
                    placeholder=""
                    dark={dark}
                    mono
                    examples={["720h", "30d", "90d"]}
                  />
                </FormField>
                <FormField
                  label="Max Size"
                  dark={dark}
                  description="The vault's disk-claim bound: oldest chunks drain past it; with Refuse on, new records refuse while over it."
                  helpRef="policy-retention#max-size"
                >
                  <TextInput
                    value={edit.maxSize}
                    onChange={(v) => setEdit(id, { maxSize: v })}
                    placeholder=""
                    dark={dark}
                    mono
                    examples={["1GB", "10GB", "100GB"]}
                  />
                </FormField>
                <FormField label="Max Chunks" dark={dark}>
                  <NumberInput
                    value={edit.maxChunks}
                    onChange={(v) => setEdit(id, { maxChunks: v })}
                    placeholder=""
                    dark={dark}
                  />
                </FormField>
              </div>
              <Checkbox
                checked={edit.refuse}
                onChange={(v) => setEdit(id, { refuse: v })}
                label={refuseDescription}
                helpRef="policy-retention#hard-vs-soft-bounds"
                dark={dark}
              />
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}
