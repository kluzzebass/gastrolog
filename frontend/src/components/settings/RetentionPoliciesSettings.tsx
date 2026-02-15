import { useState, useCallback } from "react";
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
import { UsedByStatus, refsFor } from "./UsedByStatus";
import {
  formatBytesBigint as formatBytes,
  formatDuration,
  parseBytes,
  parseDuration,
} from "../../utils/units";

interface PolicyEdit {
  maxAge: string;
  maxBytes: string;
  maxChunks: string;
}

export function RetentionPoliciesSettings({ dark }: { dark: boolean }) {
  const _c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putPolicy = usePutRetentionPolicy();
  const deletePolicy = useDeleteRetentionPolicy();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  const [newName, setNewName] = useState("");
  const [newMaxAge, setNewMaxAge] = useState("720h");
  const [newMaxBytes, setNewMaxBytes] = useState("");
  const [newMaxChunks, setNewMaxChunks] = useState("");

  const policies = config?.retentionPolicies ?? [];
  const stores = config?.stores ?? [];

  const defaults = useCallback(
    (id: string): PolicyEdit => {
      const pol = policies.find((p) => p.id === id);
      if (!pol) return { maxAge: "", maxBytes: "", maxChunks: "" };
      return {
        maxAge: formatDuration(pol.maxAgeSeconds),
        maxBytes: formatBytes(pol.maxBytes),
        maxChunks: pol.maxChunks > 0n ? pol.maxChunks.toString() : "",
      };
    },
    [policies],
  );

  const { getEdit, setEdit, clearEdit } = useEditState(defaults);

  const { handleSave: savePolicy, handleDelete } = useCrudHandlers({
    mutation: putPolicy,
    deleteMutation: deletePolicy,
    label: "Retention policy",
    onSaveTransform: (id, edit: PolicyEdit) => ({
      id,
      name: policies.find((p) => p.id === id)?.name ?? "",
      maxAgeSeconds: parseDuration(edit.maxAge),
      maxBytes: parseBytes(edit.maxBytes),
      maxChunks: edit.maxChunks ? BigInt(edit.maxChunks) : 0n,
    }),
    onDeleteSuccess: (id) => {
      const referencedBy = stores
        .filter((s) => s.retention === id)
        .map((s) => s.id);
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
    if (!newName.trim()) {
      addToast("Policy name is required", "warn");
      return;
    }
    try {
      await putPolicy.mutateAsync({
        id: "",
        name: newName.trim(),
        maxAgeSeconds: parseDuration(newMaxAge),
        maxBytes: parseBytes(newMaxBytes),
        maxChunks: newMaxChunks ? BigInt(newMaxChunks) : 0n,
      });
      addToast(`Retention policy "${newName.trim()}" created`, "info");
      setAdding(false);
      setNewName("");
      setNewMaxAge("720h");
      setNewMaxBytes("");
      setNewMaxChunks("");
    } catch (err: any) {
      addToast(err.message ?? "Failed to create retention policy", "error");
    }
  };


  return (
    <SettingsSection
      title="Retention Policies"
      addLabel="Add Policy"
      adding={adding}
      onToggleAdd={() => setAdding(!adding)}
      isLoading={isLoading}
      isEmpty={policies.length === 0}
      emptyMessage='No retention policies configured. Click "Add Policy" to create one.'
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
              label="Max Age"
              description="e.g. 720h, 30d"
              dark={dark}
            >
              <TextInput
                value={newMaxAge}
                onChange={setNewMaxAge}
                placeholder="720h"
                dark={dark}
                mono
              />
            </FormField>
            <FormField
              label="Max Bytes"
              description="e.g. 10GB, 500MB"
              dark={dark}
            >
              <TextInput
                value={newMaxBytes}
                onChange={setNewMaxBytes}
                placeholder="10GB"
                dark={dark}
                mono
              />
            </FormField>
            <FormField label="Max Chunks" dark={dark}>
              <NumberInput
                value={newMaxChunks}
                onChange={setNewMaxChunks}
                placeholder="100"
                dark={dark}
              />
            </FormField>
          </div>
        </AddFormCard>
      )}

      {policies.map((pol) => {
        const id = pol.id;
        const edit = getEdit(id);
        const refs = refsFor(stores, "retention", id);
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
                disabled={putPolicy.isPending}
              >
                {putPolicy.isPending ? "Saving..." : "Save"}
              </PrimaryButton>
            }
            status={<UsedByStatus dark={dark} refs={refs} />}
          >
            <div className="flex flex-col gap-3">
              <div className="grid grid-cols-3 gap-3">
                <FormField
                  label="Max Age"
                  description="e.g. 720h, 30d"
                  dark={dark}
                >
                  <TextInput
                    value={edit.maxAge}
                    onChange={(v) => setEdit(id, { maxAge: v })}
                    placeholder="720h"
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField
                  label="Max Bytes"
                  description="e.g. 10GB, 500MB"
                  dark={dark}
                >
                  <TextInput
                    value={edit.maxBytes}
                    onChange={(v) => setEdit(id, { maxBytes: v })}
                    placeholder="10GB"
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField label="Max Chunks" dark={dark}>
                  <NumberInput
                    value={edit.maxChunks}
                    onChange={(v) => setEdit(id, { maxChunks: v })}
                    placeholder="100"
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
