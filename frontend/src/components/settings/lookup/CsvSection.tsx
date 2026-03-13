import { useState, useEffect, useRef } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings, usePreviewCSVLookup } from "../../../api/hooks/useSettings";
import { useExpandedCard } from "../../../hooks/useExpandedCards";
import { FormField, TextInput, SelectInput } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { FileDropZone } from "../FileDropZone";
import { type CSVLookupDraft, type LookupSectionProps, emptyCsvDraft, csvLookupEqual } from "./types";
import type { CSVLookupEntry } from "../../../api/gen/gastrolog/v1/config_pb";

function serializeCsvLookups(lookups: CSVLookupDraft[]) {
  return lookups
    .filter((c) => c.name && c.fileId)
    .map((c) => ({
      name: c.name,
      fileId: c.fileId,
      keyColumn: c.keyColumn || undefined,
      valueColumns: c.valueColumns.filter(Boolean),
    }));
}

// ---------------------------------------------------------------------------
// CSV Preview + Column Pickers (shared between add form and cards)
// ---------------------------------------------------------------------------

function CsvFileFields({
  dark,
  fileId,
  keyColumn,
  valueColumns,
  onKeyColumnChange,
  onValueColumnsChange,
  managedFiles,
  uploadFile,
  addToast,
  inputId,
  onFileSelected,
}: Readonly<{
  dark: boolean;
  fileId: string;
  keyColumn: string;
  valueColumns: string[];
  onKeyColumnChange: (v: string) => void;
  onValueColumnsChange: (v: string[]) => void;
  managedFiles: LookupSectionProps["managedFiles"];
  uploadFile: LookupSectionProps["uploadFile"];
  addToast: LookupSectionProps["addToast"];
  inputId: string;
  onFileSelected: (fileId: string) => void;
}>) {
  const c = useThemeClass(dark);
  const preview = usePreviewCSVLookup();
  const lastFileIdRef = useRef("");

  // Auto-fetch preview when fileId changes.
  useEffect(() => {
    if (fileId && fileId !== lastFileIdRef.current) {
      lastFileIdRef.current = fileId;
      preview.mutate({ fileId });
    }
  }, [fileId]); // eslint-disable-line react-hooks/exhaustive-deps

  const data = preview.data;
  const columns = data && !data.error ? data.columns : [];
  const resolvedKey = keyColumn || (columns.length > 0 ? columns[0]! : "");
  const nonKeyColumns = columns.filter((col) => col !== resolvedKey);

  // Empty valueColumns means "all non-key columns" — display as all checked.
  const effectiveValues = valueColumns.length > 0 ? valueColumns : nonKeyColumns;

  const toggleValueColumn = (col: string) => {
    const isChecked = effectiveValues.includes(col);
    if (isChecked) {
      // Unchecking: if currently "all" (empty), expand to explicit list minus this one.
      const base = valueColumns.length > 0 ? valueColumns : nonKeyColumns;
      onValueColumnsChange(base.filter((v) => v !== col));
    } else {
      const next = [...valueColumns, col];
      // If all are now checked, collapse back to empty (= all).
      if (nonKeyColumns.every((c) => next.includes(c))) {
        onValueColumnsChange([]);
      } else {
        onValueColumnsChange(next);
      }
    }
  };

  return (
    <>
      <FormField label="File" dark={dark}>
        <FileDropZone
          dark={dark}
          inputId={inputId}
          accept=".csv,.tsv"
          label=".csv file"
          currentFile={fileId ? managedFiles.find((f) => f.id === fileId) : undefined}
          pickableFiles={managedFiles.filter((f) => f.name.endsWith(".csv") || f.name.endsWith(".tsv"))}
          uploadFile={uploadFile}
          addToast={addToast}
          onFileSelected={onFileSelected}
        />
      </FormField>

      {/* Preview table */}
      {fileId && (
        <div className={`rounded-lg border overflow-hidden ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
          <div className={`flex items-center justify-between px-3 py-1.5 ${c("bg-ink-surface", "bg-light-surface")}`}>
            <span className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}>
              Preview
              {data && !data.error && (
                <span className={c("text-text-ghost", "text-light-text-ghost")}>
                  {" "}&middot; {data.totalRows.toLocaleString()} rows
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

          {data && !data.error && columns.length > 0 && (
            <div className="overflow-x-auto">
              <table className="w-full text-[0.75em]">
                <thead>
                  <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
                    {columns.map((col) => (
                      <th
                        key={col}
                        className={`px-2.5 py-1.5 text-left font-mono font-medium whitespace-nowrap ${
                          col === resolvedKey
                            ? "text-copper"
                            : c("text-text-muted", "text-light-text-muted")
                        }`}
                      >
                        {col}
                        {col === resolvedKey && (
                          <span className="ml-1 text-[0.85em] opacity-60">key</span>
                        )}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.rows.map((row, ri) => (
                    <tr
                      key={ri}
                      className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
                    >
                      {row.values.map((val, ci) => (
                        <td
                          key={ci}
                          className={`px-2.5 py-1 font-mono whitespace-nowrap max-w-xs truncate ${c("text-text-bright", "text-light-text-bright")}`}
                          title={val}
                        >
                          {val}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {preview.isPending && !data && (
            <div className={`px-3 py-3 text-center text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
              Loading preview...
            </div>
          )}
        </div>
      )}

      {/* Column pickers — only shown when we have preview data */}
      {columns.length > 0 && (
        <>
          <FormField label="Key Column" description="Column used as the lookup key." dark={dark}>
            <SelectInput
              value={keyColumn}
              onChange={onKeyColumnChange}
              options={[
                { value: "", label: `${columns[0]} (first column)` },
                ...columns.slice(1).map((col) => ({ value: col, label: col })),
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
    </>
  );
}

// ---------------------------------------------------------------------------
// Add Form
// ---------------------------------------------------------------------------

export function CsvAddForm({
  dark,
  managedFiles,
  uploadFile,
  addToast,
  onCreated,
  onCancel,
  existingLookups,
  namePlaceholder,
}: LookupSectionProps & {
  onCreated: (draft: CSVLookupDraft) => void;
  onCancel: () => void;
  existingLookups: CSVLookupDraft[];
  namePlaceholder: string;
}) {
  const putConfig = usePutSettings();
  const [draft, setDraft] = useState<CSVLookupDraft>(() => emptyCsvDraft());

  const handleCreate = async () => {
    const final = { ...draft, name: draft.name.trim() || namePlaceholder };
    if (!final.name) return;
    const updated = [...existingLookups, final];
    try {
      await putConfig.mutateAsync({ lookup: { csvLookups: serializeCsvLookups(updated) } });
      onCreated(final);
      addToast(`CSV lookup "${final.name}" created`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create CSV lookup", "error");
    }
  };

  return (
    <AddFormCard
      dark={dark}
      onCancel={onCancel}
      onCreate={handleCreate}
      isPending={putConfig.isPending}
      createDisabled={!draft.name.trim() && !namePlaceholder}
      typeBadge="csv"
    >
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup assets ip" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => setDraft((d) => ({ ...d, name: v }))} placeholder={namePlaceholder} dark={dark} mono />
      </FormField>
      <CsvFileFields
        dark={dark}
        fileId={draft.fileId}
        keyColumn={draft.keyColumn}
        valueColumns={draft.valueColumns}
        onKeyColumnChange={(v) => setDraft((d) => ({ ...d, keyColumn: v }))}
        onValueColumnsChange={(v) => setDraft((d) => ({ ...d, valueColumns: v }))}
        managedFiles={managedFiles}
        uploadFile={uploadFile}
        addToast={addToast}
        inputId="new-csv-upload"
        onFileSelected={(fileId) => setDraft((d) => ({ ...d, fileId }))}
      />
    </AddFormCard>
  );
}

// ---------------------------------------------------------------------------
// Entity Cards
// ---------------------------------------------------------------------------

export function CsvCards({
  dark,
  managedFiles,
  uploadFile,
  addToast,
  lookups,
  savedLookups,
  onUpdate,
  onDelete,
}: LookupSectionProps & {
  lookups: CSVLookupDraft[];
  savedLookups: CSVLookupEntry[];
  onUpdate: (i: number, patch: Partial<CSVLookupDraft>) => void;
  onDelete: (i: number) => void;
}) {
  const c = useThemeClass(dark);
  const putConfig = usePutSettings();
  const { isExpanded, toggle } = useExpandedCard();
  const [justSaved, setJustSaved] = useState(false);

  const isDirty = (i: number) => {
    if (justSaved) return false;
    const saved = savedLookups[i];
    if (!saved) return true;
    return !csvLookupEqual(lookups[i]!, saved);
  };

  const save = async (i: number) => {
    const draft = lookups[i]!;
    try {
      await putConfig.mutateAsync({ lookup: { csvLookups: serializeCsvLookups(lookups) } });
      setJustSaved(true);
      requestAnimationFrame(() => setJustSaved(false));
      addToast(`CSV lookup "${draft.name}" saved`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to save CSV lookup", "error");
    }
  };

  const handleDelete = async (i: number) => {
    const name = lookups[i]?.name || `CSV Lookup ${i + 1}`;
    const remaining = lookups.filter((_, j) => j !== i);
    try {
      await putConfig.mutateAsync({ lookup: { csvLookups: serializeCsvLookups(remaining) } });
      onDelete(i);
      addToast(`"${name}" deleted`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to delete CSV lookup", "error");
    }
  };

  return (
    <>
      {lookups.map((cv, i) => {
        const resolvedFile = managedFiles.find((f) => f.id === cv.fileId);
        return (
          <SettingsCard
            key={`csv-${i}`}
            id={cv.name || `CSV Lookup ${i + 1}`}
            typeBadge="csv"
            dark={dark}
            expanded={isExpanded(`csv-${i}`)}
            onToggle={() => toggle(`csv-${i}`)}
            onDelete={() => handleDelete(i)}
            status={resolvedFile && (
              <span className={`font-mono text-[0.75em] truncate ${c("text-text-ghost", "text-light-text-ghost")}`} title={resolvedFile.name}>
                {resolvedFile.name}
              </span>
            )}
            footer={
              <Button
                onClick={() => save(i)}
                disabled={!isDirty(i) || !cv.name || putConfig.isPending}
              >
                {putConfig.isPending ? "Saving..." : "Save"}
              </Button>
            }
          >
            <div className="flex flex-col gap-3">
              <FormField label="Name" description="Registry name used in queries, e.g. | lookup assets ip" dark={dark}>
                <TextInput value={cv.name} onChange={(v) => onUpdate(i, { name: v })} placeholder="" dark={dark} mono />
              </FormField>
              <CsvFileFields
                dark={dark}
                fileId={cv.fileId}
                keyColumn={cv.keyColumn}
                valueColumns={cv.valueColumns}
                onKeyColumnChange={(v) => onUpdate(i, { keyColumn: v })}
                onValueColumnsChange={(v) => onUpdate(i, { valueColumns: v })}
                managedFiles={managedFiles}
                uploadFile={uploadFile}
                addToast={addToast}
                inputId={`csv-upload-${i}`}
                onFileSelected={(fileId) => onUpdate(i, { fileId })}
              />
            </div>
          </SettingsCard>
        );
      })}
    </>
  );
}
