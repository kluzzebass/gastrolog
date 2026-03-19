import { useState } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { FormField, TextInput } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { FileDropZone } from "../FileDropZone";
import { StringListEditor, ParameterListEditor } from "./FormHelpers";
import { type JSONFileLookupDraft, type LookupSectionProps, emptyJsonDraft, jsonFileLookupEqual } from "./types";
import type { JSONFileLookupEntry } from "../../../api/gen/gastrolog/v1/config_pb";

function serializeJsonLookups(lookups: JSONFileLookupDraft[]) {
  return lookups
    .filter((j) => j.name && j.fileId)
    .map((j) => ({
      name: j.name,
      fileId: j.fileId,
      query: j.query || undefined,
      responsePaths: j.responsePaths.filter(Boolean),
      parameters: j.parameters.filter((p) => p.name),
    }));
}

export function JsonAddForm({
  dark,
  managedFiles,
  uploadFile,
  addToast,
  onCreated,
  onCancel,
  existingLookups,
  namePlaceholder,
}: LookupSectionProps & {
  onCreated: (draft: JSONFileLookupDraft) => void;
  onCancel: () => void;
  existingLookups: JSONFileLookupDraft[];
  namePlaceholder: string;
}) {
  const putConfig = usePutSettings();
  const [draft, setDraft] = useState<JSONFileLookupDraft>(() => emptyJsonDraft());

  const handleCreate = async () => {
    const final = { ...draft, name: draft.name.trim() || namePlaceholder };
    if (!final.name) return;
    const updated = [...existingLookups, final];
    try {
      await putConfig.mutateAsync({ lookup: { jsonFileLookups: serializeJsonLookups(updated) } });
      onCreated(final);
      addToast(`JSON lookup "${final.name}" created`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create JSON lookup", "error");
    }
  };

  return (
    <AddFormCard
      dark={dark}
      onCancel={onCancel}
      onCreate={handleCreate}
      isPending={putConfig.isPending}
      createDisabled={!draft.name.trim() && !namePlaceholder}
      typeBadge="json"
    >
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup hosts" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => setDraft((d) => ({ ...d, name: v }))} placeholder={namePlaceholder} dark={dark} mono />
      </FormField>
      <FormField label="File" dark={dark}>
        <FileDropZone
          dark={dark}
          inputId="new-json-upload"
          accept=".json"
          label=".json file"
          currentFile={draft.fileId ? managedFiles.find((f) => f.id === draft.fileId) : undefined}
          pickableFiles={managedFiles.filter((f) => f.name.endsWith(".json"))}
          uploadFile={uploadFile}
          addToast={addToast}
          onFileSelected={(fileId) => setDraft((d) => ({ ...d, fileId }))}
        />
      </FormField>
      <FormField label="Query" description="JSONPath query with {value} or {name} placeholders. Supports filter expressions." dark={dark}>
        <TextInput value={draft.query} onChange={(v) => setDraft((d) => ({ ...d, query: v }))} placeholder="$.hosts[?(@.ip == '{value}')]" dark={dark} mono />
      </FormField>
      <ParameterListEditor values={draft.parameters} onChange={(params) => setDraft((d) => ({ ...d, parameters: params }))} dark={dark} />
      <FormField label="Response Paths" description="JSONPath expressions to extract from query results. Leave empty to flatten the entire result." dark={dark}>
        <StringListEditor values={draft.responsePaths} onChange={(v) => setDraft((d) => ({ ...d, responsePaths: v }))} placeholder="$.meta" dark={dark} />
      </FormField>
    </AddFormCard>
  );
}

export function JsonCards({
  dark,
  managedFiles,
  uploadFile,
  addToast,
  lookups,
  savedLookups,
  onUpdate,
  onDelete,
}: LookupSectionProps & {
  lookups: JSONFileLookupDraft[];
  savedLookups: JSONFileLookupEntry[];
  onUpdate: (i: number, patch: Partial<JSONFileLookupDraft>) => void;
  onDelete: (i: number) => void;
}) {
  const c = useThemeClass(dark);
  const putConfig = usePutSettings();
  const { isExpanded, toggle } = useExpandedCards();
  const [justSaved, setJustSaved] = useState(false);

  const isDirty = (i: number) => {
    if (justSaved) return false;
    const saved = savedLookups[i];
    if (!saved) return true;
    return !jsonFileLookupEqual(lookups[i]!, saved);
  };

  const save = async (i: number) => {
    const draft = lookups[i]!;
    try {
      await putConfig.mutateAsync({ lookup: { jsonFileLookups: serializeJsonLookups(lookups) } });
      setJustSaved(true);
      requestAnimationFrame(() => setJustSaved(false));
      addToast(`JSON lookup "${draft.name}" saved`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to save JSON lookup", "error");
    }
  };

  const handleDelete = async (i: number) => {
    const name = lookups[i]?.name || `JSON Lookup ${i + 1}`;
    const remaining = lookups.filter((_, j) => j !== i);
    try {
      await putConfig.mutateAsync({ lookup: { jsonFileLookups: serializeJsonLookups(remaining) } });
      onDelete(i);
      addToast(`"${name}" deleted`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to delete JSON lookup", "error");
    }
  };

  return (
    <>
      {lookups.map((j, i) => {
        const resolvedFile = managedFiles.find((f) => f.id === j.fileId);
        return (
          <SettingsCard
            key={`json-${i}`}
            id={j.name || `JSON Lookup ${i + 1}`}
            typeBadge="json"
            dark={dark}
            expanded={isExpanded(`json-${i}`)}
            onToggle={() => toggle(`json-${i}`)}
            onDelete={() => handleDelete(i)}
            status={resolvedFile && (
              <span className={`font-mono text-[0.75em] truncate ${c("text-text-ghost", "text-light-text-ghost")}`} title={resolvedFile.name}>
                {resolvedFile.name}
              </span>
            )}
            footer={
              <Button
                onClick={() => save(i)}
                disabled={!isDirty(i) || !j.name || putConfig.isPending}
              >
                {putConfig.isPending ? "Saving..." : "Save"}
              </Button>
            }
          >
            <div className="flex flex-col gap-3">
              <FormField label="Name" description="Registry name used in queries, e.g. | lookup hosts" dark={dark}>
                <TextInput value={j.name} onChange={(v) => onUpdate(i, { name: v })} placeholder="" dark={dark} mono />
              </FormField>
              <FormField label="File" dark={dark}>
                <FileDropZone
                  dark={dark}
                  inputId={`json-upload-${i}`}
                  accept=".json"
                  label=".json file"
                  currentFile={resolvedFile}
                  pickableFiles={managedFiles.filter((f) => f.name.endsWith(".json"))}
                  uploadFile={uploadFile}
                  addToast={addToast}
                  onFileSelected={(fileId) => onUpdate(i, { fileId })}
                />
              </FormField>
              <FormField label="Query" description="JSONPath query with {value} or {name} placeholders. Supports filter expressions." dark={dark}>
                <TextInput value={j.query} onChange={(v) => onUpdate(i, { query: v })} placeholder="$.hosts[?(@.ip == '{value}')]" dark={dark} mono />
              </FormField>
              <ParameterListEditor values={j.parameters} onChange={(params) => onUpdate(i, { parameters: params })} dark={dark} />
              <FormField label="Response Paths" description="JSONPath expressions to extract from query results. Leave empty to flatten the entire result." dark={dark}>
                <StringListEditor values={j.responsePaths} onChange={(v) => onUpdate(i, { responsePaths: v })} placeholder="$.meta" dark={dark} />
              </FormField>
            </div>
          </SettingsCard>
        );
      })}
    </>
  );
}
