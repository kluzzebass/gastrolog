import { encode } from "../../../api/glid";
import { useState, useEffect } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings, usePreviewJSONLookup } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { useLookupCrud } from "./useLookupCrud";
import { FormField, TextInput } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { FileDropZone } from "../FileDropZone";
import { StringListEditor, ParameterListEditor } from "./FormHelpers";
import { type JSONFileLookupDraft, type LookupSectionProps, emptyJsonDraft, jsonFileLookupEqual } from "./types";
import type { JSONFileLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";

// ---------------------------------------------------------------------------
// JSON File Preview
// ---------------------------------------------------------------------------

function JsonPreviewPanel({ dark, fileId }: Readonly<{ dark: boolean; fileId: string }>) {
  const c = useThemeClass(dark);
  const preview = usePreviewJSONLookup();

  // Auto-fetch preview when fileId changes.
  useEffect(() => {
    if (fileId) {
      preview.mutate({ fileId });
    }
  }, [fileId]); // eslint-disable-line react-hooks/exhaustive-deps

  const data = preview.data;

  if (!fileId) return null;

  return (
    <div className={`rounded-lg border overflow-hidden ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
      <div className={`flex items-center justify-between px-3 py-1.5 ${c("bg-ink-surface", "bg-light-surface")}`}>
        <span className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}>
          Preview
          {data && !data.error && (
            <span className={c("text-text-ghost", "text-light-text-ghost")}>
              {" "}&middot; {Number(data.totalSize).toLocaleString()} bytes
            </span>
          )}
        </span>
        <button
          onClick={() => preview.mutate({ fileId })}
          disabled={preview.isPending}
          className={`text-[0.7em] px-2 py-0.5 rounded transition-colors ${c(
            "text-text-ghost hover:text-copper hover:bg-ink-hover",
            "text-light-text-ghost hover:text-copper hover:bg-light-hover",
          )}`}
        >
          {preview.isPending ? "Loading..." : "Refresh"}
        </button>
      </div>

      {data?.error && (
        <div className="px-3 py-2 text-[0.8em] text-severity-error bg-severity-error/5">
          {data.error}
        </div>
      )}

      {data && !data.error && (
        <div className="overflow-x-auto">
          <pre className={`px-3 py-2 font-mono text-[0.75em] whitespace-pre ${c("text-text-bright", "text-light-text-bright")}`}>
            {data.content}
          </pre>
          {data.truncated && (
            <div className={`px-3 py-1.5 text-[0.7em] border-t ${c(
              "border-ink-border-subtle text-text-ghost",
              "border-light-border-subtle text-light-text-ghost",
            )}`}>
              Truncated
            </div>
          )}
        </div>
      )}

      {preview.isPending && !data && (
        <div className={`px-3 py-3 text-center text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
          Loading preview...
        </div>
      )}
    </div>
  );
}

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
          currentFile={draft.fileId ? managedFiles.find((f) => encode(f.id) === draft.fileId) : undefined}
          pickableFiles={managedFiles.filter((f) => f.name.endsWith(".json"))}
          uploadFile={uploadFile}
          addToast={addToast}
          onFileSelected={(fileId) => setDraft((d) => ({ ...d, fileId }))}
        />
      </FormField>
      <JsonPreviewPanel dark={dark} fileId={draft.fileId} />
      <FormField label="Query" description="JSONPath query with {value} or {name} placeholders." dark={dark}>
        <TextInput value={draft.query} onChange={(v) => setDraft((d) => ({ ...d, query: v }))} placeholder="" dark={dark} mono
          examples={["$.hosts[?(@.ip == '{value}')]", "$['{value}']", "$.data[*]"]} />
      </FormField>
      <ParameterListEditor values={draft.parameters} onChange={(params) => setDraft((d) => ({ ...d, parameters: params }))} dark={dark} />
      <FormField label="Response Paths" description="JSONPath expressions to extract from results. Leave empty to flatten all." dark={dark}>
        <StringListEditor values={draft.responsePaths} onChange={(v) => setDraft((d) => ({ ...d, responsePaths: v }))} placeholder="" dark={dark} />
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
  const { isDirty, save, handleDelete, putConfig } = useLookupCrud({
    lookups, savedLookups, serialize: serializeJsonLookups, equal: jsonFileLookupEqual,
    lookupKey: "jsonFileLookups", typeLabel: "JSON", getName: (j) => j.name, onDelete,
  });
  const { isExpanded, toggle } = useExpandedCards();

  return (
    <>
      {lookups.map((j, i) => {
        const resolvedFile = managedFiles.find((f) => encode(f.id) === j.fileId);
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
              <JsonPreviewPanel dark={dark} fileId={j.fileId} />
              <FormField label="Query" description="JSONPath query with {value} or {name} placeholders." dark={dark}>
                <TextInput value={j.query} onChange={(v) => onUpdate(i, { query: v })} placeholder="" dark={dark} mono
                  examples={["$.hosts[?(@.ip == '{value}')]", "$['{value}']", "$.data[*]"]} />
              </FormField>
              <ParameterListEditor values={j.parameters} onChange={(params) => onUpdate(i, { parameters: params })} dark={dark} />
              <FormField label="Response Paths" description="JSONPath expressions to extract from results. Leave empty to flatten all." dark={dark}>
                <StringListEditor values={j.responsePaths} onChange={(v) => onUpdate(i, { responsePaths: v })} placeholder="" dark={dark} />
              </FormField>
            </div>
          </SettingsCard>
        );
      })}
    </>
  );
}
