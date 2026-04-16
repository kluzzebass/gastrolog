import { useState } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { useLookupCrud } from "./useLookupCrud";
import { FormField, TextInput } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { type StaticLookupDraft, emptyStaticDraft, staticLookupEqual } from "./types";
import type { StaticLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";

export function serializeStaticLookups(lookups: StaticLookupDraft[]) {
  return lookups
    .filter((s) => s.name && s.keyColumn)
    .map((s) => ({
      name: s.name,
      keyColumn: s.keyColumn,
      valueColumns: s.valueColumns.filter(Boolean),
      rows: s.rows.map((r) => ({ values: r })),
    }));
}

// ---------------------------------------------------------------------------
// Inline table editor
// ---------------------------------------------------------------------------

function StaticTableEditor({
  dark,
  keyColumn,
  valueColumns,
  rows,
  onKeyColumnChange,
  onValueColumnsChange,
  onRowsChange,
}: Readonly<{
  dark: boolean;
  keyColumn: string;
  valueColumns: string[];
  rows: Record<string, string>[];
  onKeyColumnChange: (v: string) => void;
  onValueColumnsChange: (v: string[]) => void;
  onRowsChange: (v: Record<string, string>[]) => void;
}>) {
  const c = useThemeClass(dark);

  const allColumns = [keyColumn, ...valueColumns];

  const inputClass = `w-full px-2 py-1 text-[0.8em] font-mono border rounded focus:outline-none ${c(
    "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
    "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
  )}`;

  const headerClass = `px-2 py-1 text-[0.8em] font-mono font-medium border rounded focus:outline-none ${c(
    "bg-ink-surface/80 border-ink-border text-copper placeholder:text-text-ghost focus:border-copper-dim",
    "bg-light-surface/80 border-light-border text-copper placeholder:text-light-text-ghost focus:border-copper",
  )}`;

  const addColumn = () => {
    onValueColumnsChange([...valueColumns, ""]);
  };

  const removeColumn = (ci: number) => {
    const colName = valueColumns[ci]!;
    onValueColumnsChange(valueColumns.filter((_, j) => j !== ci));
    // Remove column data from rows.
    if (colName) {
      onRowsChange(rows.map((r) => {
        const next = { ...r };
        delete next[colName];
        return next;
      }));
    }
  };

  const renameColumn = (oldName: string, newName: string, isKey: boolean, colIndex?: number) => {
    if (isKey) {
      onKeyColumnChange(newName);
    } else {
      const next = [...valueColumns];
      next[colIndex!] = newName;
      onValueColumnsChange(next);
    }
    // Rename in rows.
    if (oldName && oldName !== newName) {
      onRowsChange(rows.map((r) => {
        const next = { ...r };
        if (oldName in next) {
          next[newName] = next[oldName] ?? "";
          delete next[oldName];
        }
        return next;
      }));
    }
  };

  const addRow = () => {
    const empty: Record<string, string> = {};
    for (const col of allColumns) {
      if (col) empty[col] = "";
    }
    onRowsChange([...rows, empty]);
  };

  const deleteRow = (ri: number) => {
    onRowsChange(rows.filter((_, j) => j !== ri));
  };

  const updateCell = (ri: number, col: string, value: string) => {
    const next = [...rows];
    next[ri] = { ...next[ri]!, [col]: value };
    onRowsChange(next);
  };

  return (
    <div className={`rounded-lg border overflow-hidden ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
      <div className="overflow-x-auto">
        <table className="w-full">
          {/* Column headers (editable) */}
          <thead>
            <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
              <th className="px-1.5 py-1.5 w-[1%]">
                {/* Row actions header */}
              </th>
              <th className="px-1 py-1.5">
                <input
                  type="text"
                  value={keyColumn}
                  onChange={(e) => renameColumn(keyColumn, e.target.value, true)}
                  placeholder="key"
                  className={headerClass}
                />
              </th>
              {valueColumns.map((col, ci) => (
                <th key={ci} className="px-1 py-1.5">
                  <div className="flex items-center gap-0.5">
                    <input
                      type="text"
                      value={col}
                      onChange={(e) => renameColumn(col, e.target.value, false, ci)}
                      placeholder="column"
                      className={`flex-1 min-w-0 ${headerClass}`}
                    />
                    <button
                      onClick={() => removeColumn(ci)}
                      className={`shrink-0 px-1 py-0.5 text-[0.75em] rounded transition-colors ${c(
                        "text-text-ghost hover:text-severity-error",
                        "text-light-text-ghost hover:text-severity-error",
                      )}`}
                      title="Remove column"
                    >
                      &times;
                    </button>
                  </div>
                </th>
              ))}
              <th className="px-1 py-1.5 w-[1%]">
                <button
                  onClick={addColumn}
                  className={`px-2 py-0.5 text-[0.75em] rounded border transition-colors whitespace-nowrap ${c(
                    "border-ink-border text-text-ghost hover:text-copper hover:border-copper-dim",
                    "border-light-border text-light-text-ghost hover:text-copper hover:border-copper",
                  )}`}
                  title="Add column"
                >
                  + Col
                </button>
              </th>
            </tr>
          </thead>

          {/* Data rows */}
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri} className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
                <td className="px-1.5 py-1 text-center">
                  <button
                    onClick={() => deleteRow(ri)}
                    className={`px-1 py-0.5 text-[0.75em] rounded transition-colors ${c(
                      "text-text-ghost hover:text-severity-error",
                      "text-light-text-ghost hover:text-severity-error",
                    )}`}
                    title="Delete row"
                  >
                    &times;
                  </button>
                </td>
                {allColumns.map((col, ci) => (
                  <td key={ci} className="px-1 py-1">
                    <input
                      type="text"
                      value={(col && row[col]) ?? ""}
                      onChange={(e) => col && updateCell(ri, col, e.target.value)}
                      disabled={!col}
                      className={inputClass}
                    />
                  </td>
                ))}
                <td />
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Add row button */}
      <div className={`px-3 py-1.5 border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
        <button
          onClick={addRow}
          className={`text-[0.8em] px-2 py-0.5 rounded border transition-colors ${c(
            "border-ink-border text-text-muted hover:text-copper hover:border-copper-dim hover:bg-ink-hover",
            "border-light-border text-light-text-muted hover:text-copper hover:border-copper hover:bg-light-hover",
          )}`}
        >
          + Row
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Add Form
// ---------------------------------------------------------------------------

export function StaticAddForm({
  dark,
  addToast,
  onCreated,
  onCancel,
  existingLookups,
  namePlaceholder,
}: Readonly<{
  dark: boolean;
  addToast: (msg: string, type: "info" | "error") => void;
  onCreated: (draft: StaticLookupDraft) => void;
  onCancel: () => void;
  existingLookups: StaticLookupDraft[];
  namePlaceholder: string;
}>) {
  const putConfig = usePutSettings();
  const [draft, setDraft] = useState<StaticLookupDraft>(() => emptyStaticDraft());

  const handleCreate = async () => {
    const final = { ...draft, name: draft.name.trim() || namePlaceholder };
    if (!final.name || !final.keyColumn) return;
    const updated = [...existingLookups, final];
    try {
      await putConfig.mutateAsync({ lookup: { staticLookups: serializeStaticLookups(updated) } });
      onCreated(final);
      addToast(`Static lookup "${final.name}" created`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create static lookup", "error");
    }
  };

  return (
    <AddFormCard
      dark={dark}
      onCancel={onCancel}
      onCreate={handleCreate}
      isPending={putConfig.isPending}
      createDisabled={(!draft.name.trim() && !namePlaceholder) || !draft.keyColumn.trim()}
      typeBadge="static"
    >
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup teams" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => setDraft((d) => ({ ...d, name: v }))} placeholder={namePlaceholder} dark={dark} mono />
      </FormField>
      <FormField label="Table" description="Define columns and enter rows. The first column is the lookup key." dark={dark}>
        <StaticTableEditor
          dark={dark}
          keyColumn={draft.keyColumn}
          valueColumns={draft.valueColumns}
          rows={draft.rows}
          onKeyColumnChange={(v) => setDraft((d) => ({ ...d, keyColumn: v }))}
          onValueColumnsChange={(v) => setDraft((d) => ({ ...d, valueColumns: v }))}
          onRowsChange={(v) => setDraft((d) => ({ ...d, rows: v }))}
        />
      </FormField>
    </AddFormCard>
  );
}

// ---------------------------------------------------------------------------
// Entity Cards
// ---------------------------------------------------------------------------

export function StaticCards({
  dark,
  addToast,
  lookups,
  savedLookups,
  onUpdate,
  onDelete,
}: Readonly<{
  dark: boolean;
  addToast: (msg: string, type: "info" | "error") => void;
  lookups: StaticLookupDraft[];
  savedLookups: StaticLookupEntry[];
  onUpdate: (i: number, patch: Partial<StaticLookupDraft>) => void;
  onDelete: (i: number) => void;
}>) {
  const c = useThemeClass(dark);
  const { isDirty, save, handleDelete, putConfig } = useLookupCrud({
    lookups, savedLookups, serialize: serializeStaticLookups, equal: staticLookupEqual,
    lookupKey: "staticLookups", typeLabel: "Static", getName: (s) => s.name, onDelete,
  });
  const { isExpanded, toggle } = useExpandedCards();

  return (
    <>
      {lookups.map((s, i) => (
        <SettingsCard
          key={`static-${i}`}
          id={s.name || `Static Lookup ${i + 1}`}
          typeBadge="static"
          dark={dark}
          expanded={isExpanded(`static-${i}`)}
          onToggle={() => toggle(`static-${i}`)}
          onDelete={() => handleDelete(i)}
          status={
            <span className={`font-mono text-[0.75em] truncate ${c("text-text-ghost", "text-light-text-ghost")}`}>
              {s.rows.length} row{s.rows.length !== 1 ? "s" : ""}
            </span>
          }
          footer={
            <Button
              onClick={() => save(i)}
              disabled={!isDirty(i) || !s.name || !s.keyColumn || putConfig.isPending}
            >
              {putConfig.isPending ? "Saving..." : "Save"}
            </Button>
          }
        >
          <div className="flex flex-col gap-3">
            <FormField label="Name" description="Registry name used in queries, e.g. | lookup teams" dark={dark}>
              <TextInput value={s.name} onChange={(v) => onUpdate(i, { name: v })} placeholder="" dark={dark} mono />
            </FormField>
            <FormField label="Table" description="Define columns and enter rows. The first column is the lookup key." dark={dark}>
              <StaticTableEditor
                dark={dark}
                keyColumn={s.keyColumn}
                valueColumns={s.valueColumns}
                rows={s.rows}
                onKeyColumnChange={(v) => onUpdate(i, { keyColumn: v })}
                onValueColumnsChange={(v) => onUpdate(i, { valueColumns: v })}
                onRowsChange={(v) => onUpdate(i, { rows: v })}
              />
            </FormField>
          </div>
        </SettingsCard>
      ))}
    </>
  );
}
