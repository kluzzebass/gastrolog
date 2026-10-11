import { encode } from "../../../api/glid";
import { useState, useEffect } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutLookupSettings, usePreviewJSONLookup, usePreviewYAMLLookup, type PutLookupWire } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { useLookupCrud } from "./useLookupCrud";
import { FormField, TextInput } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { FileDropZone } from "../FileDropZone";
import { ColumnPickers } from "./ColumnPickers";
import { type JSONFileLookupDraft, type LookupSectionProps, emptyJsonDraft } from "./types";
import type { JSONFileLookupEntry, YAMLFileLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";
import { PreviewTable, parseTabularResult } from "./PreviewTable";
import { useReadOnly } from "../../../hooks/useReadOnly";

// FileLookupFormat tags which structured format a section instance serves.
// Drives preview hook, file-picker extension, type badge, and payload key.
type FileLookupFormat = "json" | "yaml";

interface FormatSpec {
  format: FileLookupFormat;
  label: string;           // "JSON" / "YAML" — capitalized for user-visible text
  typeBadge: string;       // "json" / "yaml" — lowercase badge text
  helpRef: string;         // help section describing this format's lookups
  accept: string;          // accepted file extensions for the drop zone
  fileExtensions: string[]; // filter for managed-file picker
  lookupKey: "jsonFileLookups" | "yamlFileLookups"; // putLookupSettings payload field
  equal: (draft: JSONFileLookupDraft, saved: JSONFileLookupEntry | YAMLFileLookupEntry) => boolean;
  usePreview: typeof usePreviewJSONLookup;
}

// ---------------------------------------------------------------------------
// File Preview
// ---------------------------------------------------------------------------

function FilePreviewPanel({ dark, fileId, query, keyColumn, onColumnsAvailable, spec }: Readonly<{
  dark: boolean;
  fileId: string;
  query?: string;
  keyColumn?: string;
  onColumnsAvailable?: (columns: string[]) => void;
  spec: FormatSpec;
}>) {
  const readOnly = useReadOnly();
  const c = useThemeClass(dark);
  const preview = spec.usePreview();
  const [data, setData] = useState(preview.data);

  // Auto-fetch preview when fileId or query changes (debounced for query).
  const [debouncedQuery, setDebouncedQuery] = useState(query ?? "");
  useEffect(() => {
    const t = setTimeout(() => setDebouncedQuery(query ?? ""), 300);
    return () => clearTimeout(t);
  }, [query]);

  useEffect(() => {
    if (fileId) {
      preview.mutateAsync({ fileId, query: debouncedQuery }).then((d) => {
        setData(d);
        if (d.queryResult) {
          const table = parseTabularResult(d.queryResult);
          onColumnsAvailable?.(table ? table.columns : []);
        } else {
          onColumnsAvailable?.([]);
        }
      }).catch(() => {});
    }
  }, [fileId, debouncedQuery]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!fileId) return null;

  // Parse table for rendering (keyColumn highlight).
  const table = data?.queryResult ? parseTabularResult(data.queryResult) : null;
  const resolvedKey = keyColumn || (table && table.columns.length > 0 ? table.columns[0]! : "");

  return (
    <div className={`rounded-lg border overflow-hidden ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
      <div className={`flex items-center justify-between px-3 py-1.5 ${c("bg-ink-surface", "bg-light-surface")}`}>
        <span className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}>
          Preview
          {data && !data.error && (
            <span className={c("text-text-muted", "text-light-text-muted")}>
              {" "}&middot; {Number(data.totalSize).toLocaleString()} bytes
            </span>
          )}
        </span>
        <button
          onClick={() => preview.mutateAsync({ fileId, query: debouncedQuery }).then((d) => {
            setData(d);
            if (d.queryResult) {
              const t = parseTabularResult(d.queryResult);
              onColumnsAvailable?.(t ? t.columns : []);
            } else {
              onColumnsAvailable?.([]);
            }
          }).catch(() => {})}
          disabled={preview.isPending || readOnly}
          className={`text-[0.7em] px-2 py-0.5 rounded transition-colors ${c(
            "text-text-muted hover:text-copper hover:bg-ink-hover",
            "text-light-text-muted hover:text-copper hover:bg-light-hover",
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
        <div className="overflow-x-auto max-h-48">
          <pre className={`px-3 py-2 font-mono text-[0.75em] whitespace-pre ${c("text-text-bright", "text-light-text-bright")}`}>
            {data.content}
          </pre>
          {data.truncated && (
            <div className={`px-3 py-1.5 text-[0.7em] border-t ${c(
              "border-ink-border-subtle text-text-muted",
              "border-light-border-subtle text-light-text-muted",
            )}`}>
              Truncated
            </div>
          )}
        </div>
      )}

      {data?.queryResult && (() => {
        return table ? (
          <div className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
            <div className={`px-3 py-1.5 text-[0.75em] font-medium ${c("text-text-muted bg-ink-surface", "text-light-text-muted bg-light-surface")}`}>
              Query Result &middot; {table.rows.length} rows
            </div>
            <PreviewTable dark={dark} columns={table.columns} rows={table.rows} keyColumn={resolvedKey} />
          </div>
        ) : (
          <div className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
            <div className={`px-3 py-2 text-[0.75em] ${c("text-severity-warn bg-severity-warn/5", "text-severity-warn bg-severity-warn/5")}`}>
              Expression must produce an array of objects to use as a lookup table
            </div>
            <div className="overflow-x-auto max-h-32">
              <pre className={`px-3 py-2 font-mono text-[0.75em] whitespace-pre text-copper`}>
                {data.queryResult}
              </pre>
            </div>
          </div>
        );
      })()}

      {data?.queryError && (
        <div className={`border-t px-3 py-2 text-[0.75em] text-severity-error bg-severity-error/5 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
          {data.queryError}
        </div>
      )}

      {preview.isPending && !data && (
        <div className={`px-3 py-3 text-center text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
          Loading preview...
        </div>
      )}
    </div>
  );
}

function serializeFileLookups(lookups: JSONFileLookupDraft[]) {
  return lookups
    .filter((j) => j.name && j.fileId)
    .map((j) => ({
      name: j.name,
      fileId: j.fileId,
      query: j.query || undefined,
      keyColumn: j.keyColumn || undefined,
      valueColumns: j.valueColumns.filter(Boolean),
    }));
}

export function FileLookupAddForm({
  dark,
  managedFiles,
  uploadFile,
  addToast,
  onCreated,
  onCancel,
  existingLookups,
  namePlaceholder,
  spec,
}: LookupSectionProps & {
  onCreated: (draft: JSONFileLookupDraft) => void;
  onCancel: () => void;
  existingLookups: JSONFileLookupDraft[];
  namePlaceholder: string;
  spec: FormatSpec;
}) {
  const putConfig = usePutLookupSettings();
  const [draft, setDraft] = useState<JSONFileLookupDraft>(() => emptyJsonDraft());
  const [tableColumns, setTableColumns] = useState<string[]>([]);

  const resolvedKey = draft.keyColumn || (tableColumns.length > 0 ? tableColumns[0]! : "");

  const handleCreate = async () => {
    const final = { ...draft, name: draft.name.trim() || namePlaceholder };
    if (!final.name) return;
    const updated = [...existingLookups, final];
    try {
      await putConfig.mutateAsync({ [spec.lookupKey]: serializeFileLookups(updated) } as PutLookupWire);
      onCreated(final);
      addToast(`${spec.label} lookup "${final.name}" created`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : `Failed to create ${spec.label} lookup`, "error");
    }
  };

  return (
    <AddFormCard
      dark={dark}
      onCancel={onCancel}
      onCreate={handleCreate}
      isPending={putConfig.isPending}
      createDisabled={!draft.name.trim() && !namePlaceholder}
      typeBadge={spec.typeBadge}
    >
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup hosts" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => setDraft((d) => ({ ...d, name: v }))} placeholder={namePlaceholder} dark={dark} mono />
      </FormField>
      <FormField label="File" dark={dark}>
        <FileDropZone
          dark={dark}
          inputId={`new-${spec.format}-upload`}
          accept={spec.accept}
          label={`${spec.fileExtensions[0]} file`}
          currentFile={draft.fileId ? managedFiles.find((f) => encode(f.id) === draft.fileId) : undefined}
          pickableFiles={managedFiles.filter((f) => spec.fileExtensions.some((ext) => f.name.endsWith(ext)))}
          uploadFile={uploadFile}
          addToast={addToast}
          onFileSelected={(fileId) => setDraft((d) => ({ ...d, fileId }))}
        />
      </FormField>
      <FormField label="Query" description={`jq expression that transforms the ${spec.label} file into a lookup table.`} dark={dark}>
        <TextInput value={draft.query} onChange={(v) => setDraft((d) => ({ ...d, query: v }))} placeholder="" dark={dark} mono
          examples={["[.hosts[] | {ip, hostname, env}]", ".data", "[.[] | {key: .id, value: .name}]"]} />
      </FormField>
      <FilePreviewPanel dark={dark} fileId={draft.fileId} query={draft.query} keyColumn={resolvedKey} onColumnsAvailable={setTableColumns} spec={spec} />

      <ColumnPickers
        columns={tableColumns}
        keyColumn={draft.keyColumn}
        valueColumns={draft.valueColumns}
        onKeyColumnChange={(v) => setDraft((d) => ({ ...d, keyColumn: v }))}
        onValueColumnsChange={(v) => setDraft((d) => ({ ...d, valueColumns: v }))}
        unavailableReason={tableColumnsUnavailable(draft.fileId, tableColumns)}
        helpRef={spec.helpRef}
        dark={dark}
      />
    </AddFormCard>
  );
}

/** Why the column pickers have nothing to offer yet, or undefined once the query yields a table. */
export function tableColumnsUnavailable(fileId: string, tableColumns: string[]): string | undefined {
  if (!fileId) return "Select a file first — the columns come from the query result.";
  if (tableColumns.length === 0) return "The query must produce an array of objects to pick columns from.";
  return undefined;
}

function FileLookupCardBody({
  dark,
  draft,
  index,
  managedFiles,
  uploadFile,
  addToast,
  onUpdate,
  spec,
}: Readonly<{
  dark: boolean;
  draft: JSONFileLookupDraft;
  index: number;
  managedFiles: LookupSectionProps["managedFiles"];
  uploadFile: LookupSectionProps["uploadFile"];
  addToast: LookupSectionProps["addToast"];
  onUpdate: (i: number, patch: Partial<JSONFileLookupDraft>) => void;
  spec: FormatSpec;
}>) {
  const [tableColumns, setTableColumns] = useState<string[]>([]);

  const resolvedFile = managedFiles.find((f) => encode(f.id) === draft.fileId);
  const resolvedKey = draft.keyColumn || (tableColumns.length > 0 ? tableColumns[0]! : "");

  return (
    <div className="flex flex-col gap-3">
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup hosts" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => onUpdate(index, { name: v })} placeholder="" dark={dark} mono />
      </FormField>
      <FormField label="File" dark={dark}>
        <FileDropZone
          dark={dark}
          inputId={`${spec.format}-upload-${index}`}
          accept={spec.accept}
          label={`${spec.fileExtensions[0]} file`}
          currentFile={resolvedFile}
          pickableFiles={managedFiles.filter((f) => spec.fileExtensions.some((ext) => f.name.endsWith(ext)))}
          uploadFile={uploadFile}
          addToast={addToast}
          onFileSelected={(fileId) => onUpdate(index, { fileId })}
        />
      </FormField>
      <FormField label="Query" description={`jq expression that transforms the ${spec.label} file into a lookup table.`} dark={dark}>
        <TextInput value={draft.query} onChange={(v) => onUpdate(index, { query: v })} placeholder="" dark={dark} mono
          examples={["[.hosts[] | {ip, hostname, env}]", ".data", "[.[] | {key: .id, value: .name}]"]} />
      </FormField>
      <FilePreviewPanel dark={dark} fileId={draft.fileId} query={draft.query} keyColumn={resolvedKey} onColumnsAvailable={setTableColumns} spec={spec} />

      <ColumnPickers
        columns={tableColumns}
        keyColumn={draft.keyColumn}
        valueColumns={draft.valueColumns}
        onKeyColumnChange={(v) => onUpdate(index, { keyColumn: v })}
        onValueColumnsChange={(v) => onUpdate(index, { valueColumns: v })}
        unavailableReason={tableColumnsUnavailable(draft.fileId, tableColumns)}
        helpRef={spec.helpRef}
        dark={dark}
      />
    </div>
  );
}

export function FileLookupCards({
  dark,
  managedFiles,
  uploadFile,
  addToast,
  lookups,
  savedLookups,
  onUpdate,
  onDelete,
  onRevert,
  spec,
}: LookupSectionProps & {
  lookups: JSONFileLookupDraft[];
  savedLookups: (JSONFileLookupEntry | YAMLFileLookupEntry)[];
  onUpdate: (i: number, patch: Partial<JSONFileLookupDraft>) => void;
  onDelete: (i: number) => void;
  onRevert: (i: number) => void;
  spec: FormatSpec;
}) {
  const c = useThemeClass(dark);
  const { isDirty, save, handleDelete, putConfig } = useLookupCrud({
    lookups, savedLookups, serialize: serializeFileLookups, equal: spec.equal,
    lookupKey: spec.lookupKey, typeLabel: spec.label, getName: (j) => j.name, onDelete,
  });
  const { isExpanded, toggle } = useExpandedCards();

  return (
    <>
      {lookups.map((j, i) => {
        const resolvedFile = managedFiles.find((f) => encode(f.id) === j.fileId);
        return (
          <SettingsCard
            key={`${spec.format}-${i}`}
            id={j.name || `${spec.label} Lookup ${i + 1}`}
            typeBadge={spec.typeBadge}
            dark={dark}
            expanded={isExpanded(`${spec.format}-${i}`)}
            onToggle={() => toggle(`${spec.format}-${i}`)}
            onDelete={() => handleDelete(i)}
            status={resolvedFile && (
              <span className={`font-mono text-[0.75em] truncate ${c("text-text-muted", "text-light-text-muted")}`} title={resolvedFile.name}>
                {resolvedFile.name}
              </span>
            )}
            footer={
              <>
                {isDirty(i) && (
                  <Button onClick={() => onRevert(i)} disabled={putConfig.isPending} dark={dark} variant="ghost">
                    Discard
                  </Button>
                )}
                <Button
                  onClick={() => save(i)}
                  disabled={!isDirty(i) || !j.name || putConfig.isPending}
                >
                  {putConfig.isPending ? "Saving..." : "Save"}
                </Button>
              </>
            }
          >
            <FileLookupCardBody
              dark={dark}
              draft={j}
              index={i}
              managedFiles={managedFiles}
              uploadFile={uploadFile}
              addToast={addToast}
              onUpdate={onUpdate}
              spec={spec}
            />
          </SettingsCard>
        );
      })}
    </>
  );
}

// ---------------------------------------------------------------------------
// Format specs exported for wrapper sections to pass in.
// ---------------------------------------------------------------------------

// Re-export the equality helpers via the types module to keep the spec
// definition self-contained within this file.
import { jsonFileLookupEqual, yamlFileLookupEqual } from "./types";

export const jsonFormatSpec: FormatSpec = {
  format: "json",
  label: "JSON",
  typeBadge: "json",
  helpRef: "lookups-settings#json-file-lookups",
  accept: ".json",
  fileExtensions: [".json"],
  lookupKey: "jsonFileLookups",
  equal: jsonFileLookupEqual as FormatSpec["equal"],
  usePreview: usePreviewJSONLookup,
};

export const yamlFormatSpec: FormatSpec = {
  format: "yaml",
  label: "YAML",
  typeBadge: "yaml",
  helpRef: "lookups-settings#yaml-file-lookups",
  accept: ".yaml,.yml",
  fileExtensions: [".yaml", ".yml"],
  lookupKey: "yamlFileLookups",
  equal: yamlFileLookupEqual as FormatSpec["equal"],
  usePreview: usePreviewYAMLLookup,
};
