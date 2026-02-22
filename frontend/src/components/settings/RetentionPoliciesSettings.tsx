import { useState, useReducer } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import {
  useConfig,
  usePutRetentionPolicy,
  useDeleteRetentionPolicy,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, NumberInput } from "./FormField";
import { PrimaryButton } from "./Buttons";
import { UsedByStatus, ruleRefsFor } from "./UsedByStatus";
import type { SettingsTab } from "./SettingsDialog";

type NavigateTo = (tab: SettingsTab, entityName?: string) => void;
import {
  formatBytesBigint as formatBytes,
  formatDuration,
  parseBytes,
  parseDuration,
} from "../../utils/units";

interface PolicyEdit {
  name: string;
  maxAge: string;
  maxBytes: string;
  maxChunks: string;
}

// -- Reducer for "Add retention policy" form state --

interface AddRetentionFormState {
  adding: boolean;
  newName: string;
  newMaxAge: string;
  newMaxBytes: string;
  newMaxChunks: string;
}

const addRetentionFormInitial: AddRetentionFormState = {
  adding: false,
  newName: "",
  newMaxAge: "720h",
  newMaxBytes: "",
  newMaxChunks: "",
};

type AddRetentionFormAction =
  | { type: "setAdding"; value: boolean }
  | { type: "setNewName"; value: string }
  | { type: "setNewMaxAge"; value: string }
  | { type: "setNewMaxBytes"; value: string }
  | { type: "setNewMaxChunks"; value: string }
  | { type: "resetForm" };

function addRetentionFormReducer(state: AddRetentionFormState, action: AddRetentionFormAction): AddRetentionFormState {
  switch (action.type) {
    case "setAdding":
      return { ...state, adding: action.value };
    case "setNewName":
      return { ...state, newName: action.value };
    case "setNewMaxAge":
      return { ...state, newMaxAge: action.value };
    case "setNewMaxBytes":
      return { ...state, newMaxBytes: action.value };
    case "setNewMaxChunks":
      return { ...state, newMaxChunks: action.value };
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
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);

  const [addForm, dispatchAdd] = useReducer(addRetentionFormReducer, addRetentionFormInitial);
  const { adding, newName, newMaxAge, newMaxBytes, newMaxChunks } = addForm;

  const policies = config?.retentionPolicies ?? [];
  const existingNames = new Set(policies.map((p) => p.name));
  const effectiveName = newName.trim() || "default";
  const nameConflict = existingNames.has(effectiveName);
  const stores = config?.stores ?? [];

  const defaults = (id: string): PolicyEdit => {
    const pol = policies.find((p) => p.id === id);
    if (!pol) return { name: "", maxAge: "", maxBytes: "", maxChunks: "" };
    return {
      name: pol.name,
      maxAge: formatDuration(pol.maxAgeSeconds),
      maxBytes: formatBytes(pol.maxBytes),
      maxChunks: pol.maxChunks > 0n ? pol.maxChunks.toString() : "",
    };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: savePolicy, handleDelete } = useCrudHandlers({
    mutation: putPolicy,
    deleteMutation: deletePolicy,
    label: "Retention policy",
    onSaveTransform: (id, edit: PolicyEdit) => {
      const maxChunksValue = edit.maxChunks ? BigInt(edit.maxChunks) : 0n;
      return {
        id,
        name: edit.name,
        maxAgeSeconds: parseDuration(edit.maxAge),
        maxBytes: parseBytes(edit.maxBytes),
        maxChunks: maxChunksValue,
      };
    },
    onDeleteSuccess: (id) => {
      const referencedBy = stores
        .filter((s) => (s.retentionRules ?? []).some((b: { retentionPolicyId: string }) => b.retentionPolicyId === id))
        .map((s) => s.name || s.id);
      if (referencedBy.length > 0) {
        addToast(
          `Retention policy "${id}" deleted (was used by: ${referencedBy.join(", ")})`,
          "warn",
        );
      } else {
        addToast(`Retention policy "${id}" deleted`, "info");
      }
    },
    clearEdit,
  });

  const handleSave = (id: string) => savePolicy(id, getEdit(id));

  const handleCreate = async () => {
    const name = newName.trim() || "default";
    const maxChunksValue = newMaxChunks ? BigInt(newMaxChunks) : 0n;
    try {
      await putPolicy.mutateAsync({
        id: "",
        name,
        maxAgeSeconds: parseDuration(newMaxAge),
        maxBytes: parseBytes(newMaxBytes),
        maxChunks: maxChunksValue,
      });
      addToast(`Retention policy "${name}" created`, "info");
      dispatchAdd({ type: "resetForm" });
    } catch (err: any) {
      const errorMessage = err.message ?? "Failed to create retention policy";
      addToast(errorMessage, "error");
    }
  };


  return (
    <SettingsSection
      title="Retention Policies"
      helpTopicId="policy-retention"
      addLabel="Add Policy"
      adding={adding}
      onToggleAdd={() => dispatchAdd({ type: "setAdding", value: !adding })}
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
          createDisabled={nameConflict}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={(v) => dispatchAdd({ type: "setNewName", value: v })}
              placeholder="default"
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
              label="Max Bytes"
              dark={dark}
            >
              <TextInput
                value={newMaxBytes}
                onChange={(v) => dispatchAdd({ type: "setNewMaxBytes", value: v })}
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
        </AddFormCard>
      )}

      {policies.map((pol) => {
        const id = pol.id;
        const edit = getEdit(id);
        const refs = ruleRefsFor(stores, id);
        return (
          <SettingsCard
            key={id}
            id={pol.name || id}
            dark={dark}
            expanded={expanded === id}
            onToggle={() => setExpanded(expanded === id ? null : id)}
            onDelete={() => handleDelete(id)}
            footer={
              <PrimaryButton
                onClick={() => handleSave(id)}
                disabled={putPolicy.isPending || !isDirty(id)}
              >
                {putPolicy.isPending ? "Saving..." : "Save"}
              </PrimaryButton>
            }
            status={<UsedByStatus dark={dark} refs={refs} onNavigate={onNavigateTo ? (name) => onNavigateTo("stores", name) : undefined} />}
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
                  label="Max Bytes"
                      dark={dark}
                >
                  <TextInput
                    value={edit.maxBytes}
                    onChange={(v) => setEdit(id, { maxBytes: v })}
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
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}
