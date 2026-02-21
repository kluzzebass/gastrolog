import { useState, useReducer } from "react";
import {
  useConfig,
  usePutRotationPolicy,
  useDeleteRotationPolicy,
} from "../../api/hooks";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, NumberInput } from "./FormField";
import { PrimaryButton } from "./Buttons";
import { UsedByStatus, refsFor } from "./UsedByStatus";
import { validateCron, describeCron } from "../../utils/cron";
import {
  formatBytesBigint as formatBytes,
  formatDuration,
  parseBytes,
  parseDuration,
} from "../../utils/units";

interface PolicyEdit {
  name: string;
  maxBytes: string;
  maxRecords: string;
  maxAge: string;
  cron: string;
}

// -- Reducer for "Add rotation policy" form state --

interface AddRotationFormState {
  adding: boolean;
  newName: string;
  newMaxBytes: string;
  newMaxRecords: string;
  newMaxAge: string;
  newCron: string;
}

const addRotationFormInitial: AddRotationFormState = {
  adding: false,
  newName: "",
  newMaxBytes: "",
  newMaxRecords: "",
  newMaxAge: "5m",
  newCron: "",
};

type AddRotationFormAction =
  | { type: "setAdding"; value: boolean }
  | { type: "setNewName"; value: string }
  | { type: "setNewMaxBytes"; value: string }
  | { type: "setNewMaxRecords"; value: string }
  | { type: "setNewMaxAge"; value: string }
  | { type: "setNewCron"; value: string }
  | { type: "resetForm" };

function addRotationFormReducer(state: AddRotationFormState, action: AddRotationFormAction): AddRotationFormState {
  switch (action.type) {
    case "setAdding":
      return { ...state, adding: action.value };
    case "setNewName":
      return { ...state, newName: action.value };
    case "setNewMaxBytes":
      return { ...state, newMaxBytes: action.value };
    case "setNewMaxRecords":
      return { ...state, newMaxRecords: action.value };
    case "setNewMaxAge":
      return { ...state, newMaxAge: action.value };
    case "setNewCron":
      return { ...state, newCron: action.value };
    case "resetForm":
      return addRotationFormInitial;
    default:
      return state;
  }
}

function CronField({
  value,
  onChange,
  dark,
}: Readonly<{
  value: string;
  onChange: (v: string) => void;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const trimmed = value.trim();
  const validation = trimmed ? validateCron(trimmed) : null;
  const description = validation?.valid ? describeCron(trimmed) : null;

  return (
    <FormField
      label="Cron Schedule"
      description="cron: [sec] min hour dom mon dow"
      dark={dark}
    >
      <TextInput
        value={value}
        onChange={onChange}
        placeholder=""
        dark={dark}
        mono
        examples={["0 * * * *", "0 0 * * *"]}
      />
      {trimmed && validation && (
        <div className="mt-1 text-[0.75em]">
          {validation.valid ? (
            <span className={c("text-green-400", "text-green-600")}>
              {description}
            </span>
          ) : (
            <span className={c("text-red-400", "text-red-600")}>
              {validation.error}
            </span>
          )}
        </div>
      )}
    </FormField>
  );
}

export function PoliciesSettings({ dark }: Readonly<{ dark: boolean }>) {
  const _c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putPolicy = usePutRotationPolicy();
  const deletePolicy = useDeleteRotationPolicy();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);

  const [addForm, dispatchAdd] = useReducer(addRotationFormReducer, addRotationFormInitial);
  const { adding, newName, newMaxBytes, newMaxRecords, newMaxAge, newCron } = addForm;

  const policies = config?.rotationPolicies ?? [];
  const existingNames = new Set(policies.map((p) => p.name));
  const effectiveName = newName.trim() || "default";
  const nameConflict = existingNames.has(effectiveName);
  const stores = config?.stores ?? [];

  const defaults = (id: string): PolicyEdit => {
    const pol = policies.find((p) => p.id === id);
    if (!pol) return { name: "", maxBytes: "", maxRecords: "", maxAge: "", cron: "" };
    return {
      name: pol.name,
      maxBytes: formatBytes(pol.maxBytes),
      maxRecords: pol.maxRecords > 0n ? pol.maxRecords.toString() : "",
      maxAge: formatDuration(pol.maxAgeSeconds),
      cron: pol.cron,
    };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: savePolicy, handleDelete } = useCrudHandlers({
    mutation: putPolicy,
    deleteMutation: deletePolicy,
    label: "Policy",
    onSaveTransform: (id, edit: PolicyEdit) => {
      if (edit.cron) {
        const result = validateCron(edit.cron);
        if (!result.valid) throw new Error(`Invalid cron: ${result.error}`);
      }
      const maxRecordsValue = edit.maxRecords ? BigInt(edit.maxRecords) : 0n;
      return {
        id,
        name: edit.name,
        maxBytes: parseBytes(edit.maxBytes),
        maxRecords: maxRecordsValue,
        maxAgeSeconds: parseDuration(edit.maxAge),
        cron: edit.cron,
      };
    },
    onDeleteSuccess: (id) => {
      const referencedBy = stores
        .filter((s) => s.policy === id)
        .map((s) => s.name || s.id);
      if (referencedBy.length > 0) {
        addToast(
          `Policy "${id}" deleted (was used by: ${referencedBy.join(", ")})`,
          "warn",
        );
      } else {
        addToast(`Policy "${id}" deleted`, "info");
      }
    },
    clearEdit,
  });

  const handleSave = (id: string) => savePolicy(id, getEdit(id));

  const handleCreate = async () => {
    if (newCron) {
      const result = validateCron(newCron);
      if (!result.valid) {
        addToast(`Invalid cron: ${result.error}`, "error");
        return;
      }
    }
    const name = newName.trim() || "default";
    const maxRecordsValue = newMaxRecords ? BigInt(newMaxRecords) : 0n;
    try {
      await putPolicy.mutateAsync({
        id: "",
        name,
        maxBytes: parseBytes(newMaxBytes),
        maxRecords: maxRecordsValue,
        maxAgeSeconds: parseDuration(newMaxAge),
        cron: newCron,
      });
      addToast(`Policy "${name}" created`, "info");
      dispatchAdd({ type: "resetForm" });
    } catch (err: any) {
      const errorMessage = err.message ?? "Failed to create policy";
      addToast(errorMessage, "error");
    }
  };


  return (
    <SettingsSection
      title="Rotation Policies"
      helpTopicId="policy-rotation"
      addLabel="Add Policy"
      adding={adding}
      onToggleAdd={() => dispatchAdd({ type: "setAdding", value: !adding })}
      isLoading={isLoading}
      isEmpty={policies.length === 0}
      emptyMessage='No rotation policies configured. Click "Add Policy" to create one.'
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
              label="Max Bytes"
              dark={dark}
            >
              <TextInput
                value={newMaxBytes}
                onChange={(v) => dispatchAdd({ type: "setNewMaxBytes", value: v })}
                placeholder=""
                dark={dark}
                mono
                examples={["64MB", "256MB", "1GB"]}
              />
            </FormField>
            <FormField label="Max Records" dark={dark}>
              <NumberInput
                value={newMaxRecords}
                onChange={(v) => dispatchAdd({ type: "setNewMaxRecords", value: v })}
                placeholder=""
                dark={dark}
              />
            </FormField>
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
                examples={["1h", "24h", "7d"]}
              />
            </FormField>
          </div>
          <CronField value={newCron} onChange={(v) => dispatchAdd({ type: "setNewCron", value: v })} dark={dark} />
        </AddFormCard>
      )}

      {policies.map((pol) => {
        const id = pol.id;
        const edit = getEdit(id);
        const refs = refsFor(stores, "policy", id);
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
            status={<UsedByStatus dark={dark} refs={refs} />}
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
                  label="Max Bytes"
                      dark={dark}
                >
                  <TextInput
                    value={edit.maxBytes}
                    onChange={(v) => setEdit(id, { maxBytes: v })}
                    placeholder=""
                    dark={dark}
                    mono
                    examples={["64MB", "256MB", "1GB"]}
                  />
                </FormField>
                <FormField label="Max Records" dark={dark}>
                  <NumberInput
                    value={edit.maxRecords}
                    onChange={(v) => setEdit(id, { maxRecords: v })}
                    placeholder=""
                    dark={dark}
                  />
                </FormField>
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
                    examples={["1h", "24h", "7d"]}
                  />
                </FormField>
              </div>
              <CronField
                value={edit.cron}
                onChange={(v) => setEdit(id, { cron: v })}
                dark={dark}
              />
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}
