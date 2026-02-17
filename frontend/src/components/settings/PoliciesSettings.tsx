import { useState, useCallback } from "react";
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
  maxBytes: string;
  maxRecords: string;
  maxAge: string;
  cron: string;
}

function CronField({
  value,
  onChange,
  dark,
}: {
  value: string;
  onChange: (v: string) => void;
  dark: boolean;
}) {
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
        placeholder="0 * * * *"
        dark={dark}
        mono
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

export function PoliciesSettings({ dark }: { dark: boolean }) {
  const _c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putPolicy = usePutRotationPolicy();
  const deletePolicy = useDeleteRotationPolicy();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  const [newName, setNewName] = useState("");
  const [newMaxBytes, setNewMaxBytes] = useState("");
  const [newMaxRecords, setNewMaxRecords] = useState("");
  const [newMaxAge, setNewMaxAge] = useState("5m");
  const [newCron, setNewCron] = useState("");

  const policies = config?.rotationPolicies ?? [];
  const stores = config?.stores ?? [];

  const defaults = useCallback(
    (id: string): PolicyEdit => {
      const pol = policies.find((p) => p.id === id);
      if (!pol) return { maxBytes: "", maxRecords: "", maxAge: "", cron: "" };
      return {
        maxBytes: formatBytes(pol.maxBytes),
        maxRecords: pol.maxRecords > 0n ? pol.maxRecords.toString() : "",
        maxAge: formatDuration(pol.maxAgeSeconds),
        cron: pol.cron,
      };
    },
    [policies],
  );

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
      return {
        id,
        name: policies.find((p) => p.id === id)?.name ?? "",
        maxBytes: parseBytes(edit.maxBytes),
        maxRecords: edit.maxRecords ? BigInt(edit.maxRecords) : 0n,
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
    if (!newName.trim()) {
      addToast("Policy name is required", "warn");
      return;
    }
    if (newCron) {
      const result = validateCron(newCron);
      if (!result.valid) {
        addToast(`Invalid cron: ${result.error}`, "error");
        return;
      }
    }
    try {
      await putPolicy.mutateAsync({
        id: "",
        name: newName.trim(),
        maxBytes: parseBytes(newMaxBytes),
        maxRecords: newMaxRecords ? BigInt(newMaxRecords) : 0n,
        maxAgeSeconds: parseDuration(newMaxAge),
        cron: newCron,
      });
      addToast(`Policy "${newName.trim()}" created`, "info");
      setAdding(false);
      setNewName("");
      setNewMaxBytes("");
      setNewMaxRecords("");
      setNewMaxAge("5m");
      setNewCron("");
    } catch (err: any) {
      addToast(err.message ?? "Failed to create policy", "error");
    }
  };


  return (
    <SettingsSection
      title="Rotation Policies"
      helpTopicId="policy-rotation"
      addLabel="Add Policy"
      adding={adding}
      onToggleAdd={() => setAdding(!adding)}
      isLoading={isLoading}
      isEmpty={policies.length === 0}
      emptyMessage='No rotation policies configured. Click "Add Policy" to create one.'
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => setAdding(false)}
          onCreate={handleCreate}
          isPending={putPolicy.isPending}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={setNewName}
              placeholder="default"
              dark={dark}
            />
          </FormField>
          <div className="grid grid-cols-3 gap-3">
            <FormField
              label="Max Bytes"
              description="e.g. 64MB, 1GB"
              dark={dark}
            >
              <TextInput
                value={newMaxBytes}
                onChange={setNewMaxBytes}
                placeholder="64MB"
                dark={dark}
                mono
              />
            </FormField>
            <FormField label="Max Records" dark={dark}>
              <NumberInput
                value={newMaxRecords}
                onChange={setNewMaxRecords}
                placeholder="100000"
                dark={dark}
              />
            </FormField>
            <FormField
              label="Max Age"
              description="e.g. 5m, 1h"
              dark={dark}
            >
              <TextInput
                value={newMaxAge}
                onChange={setNewMaxAge}
                placeholder="5m"
                dark={dark}
                mono
              />
            </FormField>
          </div>
          <CronField value={newCron} onChange={setNewCron} dark={dark} />
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
              <div className="grid grid-cols-3 gap-3">
                <FormField
                  label="Max Bytes"
                  description="e.g. 64MB, 1GB"
                  dark={dark}
                >
                  <TextInput
                    value={edit.maxBytes}
                    onChange={(v) => setEdit(id, { maxBytes: v })}
                    placeholder="64MB"
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField label="Max Records" dark={dark}>
                  <NumberInput
                    value={edit.maxRecords}
                    onChange={(v) => setEdit(id, { maxRecords: v })}
                    placeholder="100000"
                    dark={dark}
                  />
                </FormField>
                <FormField
                  label="Max Age"
                  description="e.g. 5m, 1h"
                  dark={dark}
                >
                  <TextInput
                    value={edit.maxAge}
                    onChange={(v) => setEdit(id, { maxAge: v })}
                    placeholder="5m"
                    dark={dark}
                    mono
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
