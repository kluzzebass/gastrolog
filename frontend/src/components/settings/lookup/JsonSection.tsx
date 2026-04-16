import { encode } from "../../../api/glid";
import { useState, useEffect } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings, usePreviewJSONLookup } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { useLookupCrud } from "./useLookupCrud";
import { FormField, TextInput, SelectInput } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { FileDropZone } from "../FileDropZone";
import { type JSONFileLookupDraft, type LookupSectionProps, emptyJsonDraft, jsonFileLookupEqual } from "./types";
import type { JSONFileLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";
import { PreviewTable, parseTabularResult } from "./PreviewTable";

// ---------------------------------------------------------------------------
// JSON File Preview
// ---------------------------------------------------------------------------

function JsonPreviewPanel({ dark, fileId, query, keyColumn, onColumnsAvailable }: Readonly<{
  dark: boolean;
  fileId: string;
  query?: string;
  keyColumn?: string;
  onColumnsAvailable?: (columns: string[]) => void;
}>) {
  const c = useThemeClass(dark);
  const preview = usePreviewJSONLookup();
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
            <span className={c("text-text-ghost", "text-light-text-ghost")}>
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
        <div className="overflow-x-auto max-h-48">
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
        <div className={`px-3 py-3 text-center text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
          Loading preview...
        </div>
      )}
    </div>
  );
}

export function serializeJsonLookups(lookups: JSONFileLookupDraft[]) {
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
  const c = useThemeClass(dark);
  const putConfig = usePutSettings();
  const [draft, setDraft] = useState<JSONFileLookupDraft>(() => emptyJsonDraft());
  const [tableColumns, setTableColumns] = useState<string[]>([]);

  const resolvedKey = draft.keyColumn || (tableColumns.length > 0 ? tableColumns[0]! : "");
  const nonKeyColumns = tableColumns.filter((col) => col !== resolvedKey);
  const effectiveValues = draft.valueColumns.length > 0 ? draft.valueColumns : nonKeyColumns;

  const toggleValueColumn = (col: string) => {
    const isChecked = effectiveValues.includes(col);
    if (isChecked) {
      const base = draft.valueColumns.length > 0 ? draft.valueColumns : nonKeyColumns;
      setDraft((d) => ({ ...d, valueColumns: base.filter((v) => v !== col) }));
    } else {
      const next = [...draft.valueColumns, col];
      if (nonKeyColumns.every((c) => next.includes(c))) {
        setDraft((d) => ({ ...d, valueColumns: [] }));
      } else {
        setDraft((d) => ({ ...d, valueColumns: next }));
      }
    }
  };

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
      <FormField label="Query" description="jq expression that transforms the JSON file into a lookup table." dark={dark}>
        <TextInput value={draft.query} onChange={(v) => setDraft((d) => ({ ...d, query: v }))} placeholder="" dark={dark} mono
          examples={["[.hosts[] | {ip, hostname, env}]", ".data", "[.[] | {key: .id, value: .name}]"]} />
      </FormField>
      <JsonPreviewPanel dark={dark} fileId={draft.fileId} query={draft.query} keyColumn={resolvedKey} onColumnsAvailable={setTableColumns} />

      {/* Column pickers — only shown when the jq expression produces a valid table */}
      {tableColumns.length > 0 && (
        <>
          <FormField label="Key Column" description="Column used as the lookup key." dark={dark}>
            <SelectInput
              value={draft.keyColumn}
              onChange={(v) => setDraft((d) => ({ ...d, keyColumn: v }))}
              options={[
                { value: "", label: `${tableColumns[0]} (first column)` },
                ...tableColumns.slice(1).map((col) => ({ value: col, label: col })),
              ]}
              dark={dark}
            />
          </FormField>

          <FormField label="Value Columns" description="Columns included in lookup results. Uncheck columns you don't need." dark={dark}>
            <div className="flex flex-wrap gap-x-3 gap-y-1.5">
              {nonKeyColumns.map((col) => {
                const checked = effectiveValues.includes(col);
                return (
                  <label
                    key={col}
                    className={`flex items-center gap-1.5 text-[0.85em] font-mono cursor-pointer select-none ${c("text-text-bright", "text-light-text-bright")}`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleValueColumn(col)}
                      className="accent-copper"
                    />
                    {col}
                  </label>
                );
              })}
            </div>
          </FormField>
        </>
      )}
    </AddFormCard>
  );
}

function JsonCardBody({
  dark,
  draft,
  index,
  managedFiles,
  uploadFile,
  addToast,
  onUpdate,
}: Readonly<{
  dark: boolean;
  draft: JSONFileLookupDraft;
  index: number;
  managedFiles: LookupSectionProps["managedFiles"];
  uploadFile: LookupSectionProps["uploadFile"];
  addToast: LookupSectionProps["addToast"];
  onUpdate: (i: number, patch: Partial<JSONFileLookupDraft>) => void;
}>) {
  const c = useThemeClass(dark);
  const [tableColumns, setTableColumns] = useState<string[]>([]);

  const resolvedFile = managedFiles.find((f) => encode(f.id) === draft.fileId);
  const resolvedKey = draft.keyColumn || (tableColumns.length > 0 ? tableColumns[0]! : "");
  const nonKeyColumns = tableColumns.filter((col) => col !== resolvedKey);
  const effectiveValues = draft.valueColumns.length > 0 ? draft.valueColumns : nonKeyColumns;

  const toggleValueColumn = (col: string) => {
    const isChecked = effectiveValues.includes(col);
    if (isChecked) {
      const base = draft.valueColumns.length > 0 ? draft.valueColumns : nonKeyColumns;
      onUpdate(index, { valueColumns: base.filter((v) => v !== col) });
    } else {
      const next = [...draft.valueColumns, col];
      if (nonKeyColumns.every((c) => next.includes(c))) {
        onUpdate(index, { valueColumns: [] });
      } else {
        onUpdate(index, { valueColumns: next });
      }
    }
  };

  return (
    <div className="flex flex-col gap-3">
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup hosts" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => onUpdate(index, { name: v })} placeholder="" dark={dark} mono />
      </FormField>
      <FormField label="File" dark={dark}>
        <FileDropZone
          dark={dark}
          inputId={`json-upload-${index}`}
          accept=".json"
          label=".json file"
          currentFile={resolvedFile}
          pickableFiles={managedFiles.filter((f) => f.name.endsWith(".json"))}
          uploadFile={uploadFile}
          addToast={addToast}
          onFileSelected={(fileId) => onUpdate(index, { fileId })}
        />
      </FormField>
      <FormField label="Query" description="jq expression that transforms the JSON file into a lookup table." dark={dark}>
        <TextInput value={draft.query} onChange={(v) => onUpdate(index, { query: v })} placeholder="" dark={dark} mono
          examples={["[.hosts[] | {ip, hostname, env}]", ".data", "[.[] | {key: .id, value: .name}]"]} />
      </FormField>
      <JsonPreviewPanel dark={dark} fileId={draft.fileId} query={draft.query} keyColumn={resolvedKey} onColumnsAvailable={setTableColumns} />

      {/* Column pickers — only shown when the jq expression produces a valid table */}
      {tableColumns.length > 0 && (
        <>
          <FormField label="Key Column" description="Column used as the lookup key." dark={dark}>
            <SelectInput
              value={draft.keyColumn}
              onChange={(v) => onUpdate(index, { keyColumn: v })}
              options={[
                { value: "", label: `${tableColumns[0]} (first column)` },
                ...tableColumns.slice(1).map((col) => ({ value: col, label: col })),
              ]}
              dark={dark}
            />
          </FormField>

          <FormField label="Value Columns" description="Columns included in lookup results. Uncheck columns you don't need." dark={dark}>
            <div className="flex flex-wrap gap-x-3 gap-y-1.5">
              {nonKeyColumns.map((col) => {
                const checked = effectiveValues.includes(col);
                return (
                  <label
                    key={col}
                    className={`flex items-center gap-1.5 text-[0.85em] font-mono cursor-pointer select-none ${c("text-text-bright", "text-light-text-bright")}`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleValueColumn(col)}
                      className="accent-copper"
                    />
                    {col}
                  </label>
                );
              })}
            </div>
          </FormField>
        </>
      )}
    </div>
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
            <JsonCardBody
              dark={dark}
              draft={j}
              index={i}
              managedFiles={managedFiles}
              uploadFile={uploadFile}
              addToast={addToast}
              onUpdate={onUpdate}
            />
          </SettingsCard>
        );
      })}
    </>
  );
}
