import { useState } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { useLookupCrud } from "./useLookupCrud";
import { FormField, TextInput } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { EditableGrid } from "../EditableGrid";
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

// Returns true if any row has an empty or duplicate key value.
function hasKeyErrors(rows: Record<string, string>[], keyColumn: string): boolean {
  if (!keyColumn) return false;
  const seen = new Set<string>();
  for (const r of rows) {
    const v = (r[keyColumn] ?? "").trim();
    if (!v || seen.has(v)) return true;
    seen.add(v);
  }
  return false;
}

// Returns true if any column header (key or value) is empty. Empty headers
// get silently dropped by serializeStaticLookups, so the UI must block
// Create/Save while they're present — otherwise the user saves something
// visibly different from what they typed.
function hasEmptyColumns(keyColumn: string, valueColumns: string[]): boolean {
  if (!keyColumn.trim()) return true;
  return valueColumns.some((c) => !c.trim());
}

// Helper: merge keyColumn + valueColumns into a single array for EditableGrid.
function mergeColumns(key: string, values: string[]): string[] {
  return [key, ...values];
}

// Helper: split EditableGrid's columns array back into keyColumn + valueColumns.
function splitColumns(cols: string[]): { keyColumn: string; valueColumns: string[] } {
  return { keyColumn: cols[0] ?? "", valueColumns: cols.slice(1) };
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
    if (!final.name) return;
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
      createDisabled={(!draft.name.trim() && !namePlaceholder) || hasEmptyColumns(draft.keyColumn, draft.valueColumns) || hasKeyErrors(draft.rows, draft.keyColumn)}
      typeBadge="static"
    >
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup teams" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => setDraft((d) => ({ ...d, name: v }))} placeholder={namePlaceholder} dark={dark} mono />
      </FormField>
      <EditableGrid
          dark={dark}
          columns={mergeColumns(draft.keyColumn, draft.valueColumns)}
          rows={draft.rows}
          onColumnsChange={(cols) => {
            const { keyColumn, valueColumns } = splitColumns(cols);
            setDraft((d) => ({ ...d, keyColumn, valueColumns }));
          }}
          onRowsChange={(rows) => setDraft((d) => ({ ...d, rows }))}
        />
    </AddFormCard>
  );
}

// ---------------------------------------------------------------------------
// Entity Cards
// ---------------------------------------------------------------------------

export function StaticCards({
  dark,
  lookups,
  savedLookups,
  onUpdate,
  onDelete,
  onRevert,
}: Readonly<{
  dark: boolean;
  lookups: StaticLookupDraft[];
  savedLookups: StaticLookupEntry[];
  onUpdate: (i: number, patch: Partial<StaticLookupDraft>) => void;
  onDelete: (i: number) => void;
  onRevert: (i: number) => void;
}>) {
  const c = useThemeClass(dark);
  const { isDirty, save, handleDelete, putConfig } = useLookupCrud({
    lookups, savedLookups, serialize: serializeStaticLookups, equal: staticLookupEqual,
    lookupKey: "staticLookups", typeLabel: "Static", getName: (s) => s.name, onDelete,
  });
  const { isExpanded, toggle } = useExpandedCards();

  return (
    <>
      {lookups.map((s, i) => {
        const cardKey = s.name || `static-${String(i)}`;
        return (
        <SettingsCard
          key={cardKey}
          id={s.name || `Static Lookup ${String(i + 1)}`}
          typeBadge="static"
          dark={dark}
          expanded={isExpanded(cardKey)}
          onToggle={() => toggle(cardKey)}
          onDelete={() => handleDelete(i)}
          status={
            <span className={`font-mono text-[0.75em] truncate ${c("text-text-muted", "text-light-text-muted")}`}>
              {s.rows.length} row{s.rows.length === 1 ? "" : "s"}
            </span>
          }
          footer={
            <>
              {isDirty(i) && (
                <Button onClick={() => onRevert(i)} disabled={putConfig.isPending} dark={dark} variant="ghost">
                  Discard
                </Button>
              )}
              <Button
                onClick={() => save(i)}
                disabled={!isDirty(i) || !s.name || hasEmptyColumns(s.keyColumn, s.valueColumns) || putConfig.isPending || hasKeyErrors(s.rows, s.keyColumn)}
              >
                {putConfig.isPending ? "Saving..." : "Save"}
              </Button>
            </>
          }
        >
          <div className="flex flex-col gap-3">
            <FormField label="Name" description="Registry name used in queries, e.g. | lookup teams" dark={dark}>
              <TextInput value={s.name} onChange={(v) => onUpdate(i, { name: v })} placeholder="" dark={dark} mono />
            </FormField>
              <EditableGrid
                dark={dark}
                columns={mergeColumns(s.keyColumn, s.valueColumns)}
                rows={s.rows}
                onColumnsChange={(cols) => {
                  const { keyColumn, valueColumns } = splitColumns(cols);
                  onUpdate(i, { keyColumn, valueColumns });
                }}
                onRowsChange={(rows) => onUpdate(i, { rows })}
              />
          </div>
        </SettingsCard>
        );
      })}
    </>
  );
}
