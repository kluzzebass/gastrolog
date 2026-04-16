/**
 * EditableGrid — inline table editor with drag-to-reorder columns and rows.
 *
 * Row reordering: full @dnd-kit/sortable with DragOverlay for a visual ghost.
 * Column reordering: pointer-capture drag with a dedicated grip handle.
 * - Drag initiates from anywhere in the row that isn't an input or button.
 * - Grip cell is a visual affordance + keyboard accessibility target.
 * - Inputs stop pointer propagation so clicks don't trigger drag.
 * - Ghost: elevated clone with copper border + shadow.
 */
import { useState, useRef } from "react";
import { createPortal } from "react-dom";
import {
  DndContext,
  DragOverlay,
  PointerSensor,
  KeyboardSensor,
  closestCenter,
  useSensor,
  useSensors,
  type DragEndEvent,
  type DragStartEvent,
} from "@dnd-kit/core";
import {
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
  arrayMove,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { useThemeClass } from "../../hooks/useThemeClass";

export interface EditableGridProps {
  dark: boolean;
  columns: string[];
  rows: Record<string, string>[];
  keyColumnIndex?: number;
  readOnly?: boolean;
  onColumnsChange?: (columns: string[]) => void;
  onRowsChange?: (rows: Record<string, string>[]) => void;
}

// Small key icon — marks the lookup key column. Drawn at -45° for a classic diagonal look.
function KeyIcon({ className }: { readonly className?: string }) {
  return (
    <svg
      width="12" height="12" viewBox="0 0 16 16"
      fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"
      className={className}
      aria-label="key column"
    >
      <g transform="rotate(45, 8, 8)">
        <circle cx="5" cy="8" r="2.5" />
        <path d="M7.5 8h7M12 8v1.5M14 8v1.5" />
      </g>
    </svg>
  );
}

// Six-dot grip icon.
function GripDots({ className }: { readonly className?: string }) {
  return (
    <svg width="8" height="12" viewBox="0 0 8 12" fill="currentColor" className={className} aria-hidden="true">
      <circle cx="2" cy="2" r="1.2" />
      <circle cx="6" cy="2" r="1.2" />
      <circle cx="2" cy="6" r="1.2" />
      <circle cx="6" cy="6" r="1.2" />
      <circle cx="2" cy="10" r="1.2" />
      <circle cx="6" cy="10" r="1.2" />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Key validation — detects empty and duplicate key values.
// ---------------------------------------------------------------------------

function validateKeys(
  keyCol: string,
  rows: Record<string, string>[],
  ids: string[],
): { emptyKeyIds: Set<string>; duplicateKeyIds: Set<string> } {
  const emptyKeyIds = new Set<string>();
  const duplicateKeyIds = new Set<string>();
  if (!keyCol) return { emptyKeyIds, duplicateKeyIds };

  const seen = new Map<string, string[]>();
  for (let i = 0; i < rows.length; i++) {
    const id = ids[i]!; // NOSONAR — ids.length === rows.length invariant
    const val = (rows[i]?.[keyCol] ?? "").trim();
    if (val) {
      const group = seen.get(val);
      if (group) { group.push(id); } else { seen.set(val, [id]); }
    } else {
      emptyKeyIds.add(id);
    }
  }
  for (const group of seen.values()) {
    if (group.length > 1) for (const id of group) duplicateKeyIds.add(id);
  }
  return { emptyKeyIds, duplicateKeyIds };
}

// ---------------------------------------------------------------------------
// Sortable row
// ---------------------------------------------------------------------------

interface SortableRowProps {
  id: string;
  row: Record<string, string>;
  columns: string[];
  keyColumnIndex: number;
  keyError: "empty" | "duplicate" | null;
  inputClass: string;
  inputErrorClass: string;
  inputsDisabled: boolean;
  onDelete: () => void;
  onAddRow: () => void;
  onCellChange: (col: string, val: string) => void;
  deleteClass: string;
  addClass: string;
  borderClass: string;
}

function SortableRow({
  id, row, columns, keyColumnIndex, keyError, inputClass, inputErrorClass, inputsDisabled, onDelete, onAddRow, onCellChange, deleteClass, addClass, borderClass,
}: Readonly<SortableRowProps>) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

  return (
    // listeners goes on <tr> so the whole row is a drag surface.
    // attributes (aria-*) goes on the grip div for keyboard accessibility.
    <tr
      ref={setNodeRef}
      style={{ transform: CSS.Transform.toString(transform), transition }}
      className={`group border-t cursor-grab active:cursor-grabbing ${borderClass} ${isDragging ? "opacity-30" : ""}`}
      {...listeners}
    >
      {columns.map((col, ci) => (
        <td key={col || `col-${ci}`} className="px-1 py-1">
          <div className="flex items-center gap-0.5">
            {ci === 0 && (
              <div
                {...attributes}
                className="shrink-0 px-0.5 touch-none text-text-muted group-hover:text-copper transition-colors"
                title="Drag to reorder"
              >
                <GripDots />
              </div>
            )}
            <input
              type="text"
              value={(col && row[col]) ?? ""}
              onChange={(e) => col && onCellChange(col, e.target.value)}
              onPointerDown={(e) => e.stopPropagation()}
              disabled={!col || inputsDisabled}
              className={`flex-1 min-w-0 cursor-text ${ci === keyColumnIndex && keyError ? inputErrorClass : inputClass}`}
            />
          </div>
        </td>
      ))}

      {/* Delete row */}
      <td className="w-7 px-1 py-1 text-center">
        <button
          onClick={onDelete}
          onPointerDown={(e) => e.stopPropagation()}
          className={`p-1.5 rounded cursor-pointer text-base font-medium leading-none ${deleteClass}`}
          title="Delete row"
        >
          ×
        </button>
      </td>

      {/* Add column */}
      <td className="w-7 px-1 py-1 text-center">
        <button
          onClick={onAddRow}
          onPointerDown={(e) => e.stopPropagation()}
          className={`p-1.5 rounded cursor-pointer text-[0.9em] font-medium leading-none ${addClass}`}
          title="Add column"
        >
          +
        </button>
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Drag overlay — floating ghost during row drag.
// ---------------------------------------------------------------------------

interface RowOverlayProps {
  row: Record<string, string>;
  columns: string[];
  overlayClass: string;
  keyClass: string;
  valClass: string;
}

function RowOverlay({ row, columns, overlayClass, keyClass, valClass }: Readonly<RowOverlayProps>) {
  return (
    <div className={`flex items-center gap-1 rounded-lg border-2 px-1.5 py-1.5 shadow-2xl scale-[1.02] ${overlayClass}`}>
      <GripDots className="shrink-0 mx-0.5 text-copper" />
      {columns.map((col, ci) => (
        <div
          key={col || `col-${ci}`}
          className={`flex-1 min-w-0 text-[0.8em] font-mono truncate px-2 py-0.5 rounded border ${ci === 0 ? keyClass : valClass}`}
        >
          {col ? (row[col] ?? "") : ""}
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// EditableGrid
// ---------------------------------------------------------------------------

export function EditableGrid({
  dark, columns, rows, keyColumnIndex = 0, readOnly = false, onColumnsChange, onRowsChange,
}: Readonly<EditableGridProps>) {
  const c = useThemeClass(dark);

  // ── Stable row IDs ────────────────────────────────────────────────────────
  // IDs are assigned once at row creation and survive reorders/deletes.
  // Using stable IDs (not array indices) as React keys prevents remounting
  // on reorder and avoids the S6479 linter warning.
  const stableIds = useRef<string[]>([]);
  const idSeq = useRef(0);

  // Sync ID list length with rows. Only grows/shrinks; explicit reorders and
  // deletes are handled in handleDragEnd and deleteRow respectively.
  const ids = stableIds.current;
  while (ids.length < rows.length) ids.push(`r${idSeq.current++}`);
  if (ids.length > rows.length) ids.splice(rows.length);

  // ── Row dnd-kit ──────────────────────────────────────────────────────────
  const [activeRowId, setActiveRowId] = useState<string | null>(null);
  const activeRowIdx = activeRowId ? ids.indexOf(activeRowId) : -1;

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  const handleDragStart = (event: DragStartEvent) => {
    setActiveRowId(event.active.id as string);
  };

  const handleDragEnd = (event: DragEndEvent) => {
    setActiveRowId(null);
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    const oldIdx = ids.indexOf(active.id as string);
    const newIdx = ids.indexOf(over.id as string);
    if (oldIdx === -1 || newIdx === -1) return;
    stableIds.current = arrayMove([...ids], oldIdx, newIdx);
    onRowsChange?.(arrayMove(rows, oldIdx, newIdx));
  };

  // ── Column pointer-drag ──────────────────────────────────────────────────
  const [dragCols, setDragCols] = useState<string[] | null>(null);
  const [dragColIdx, setDragColIdx] = useState<number | null>(null);
  const [colGhost, setColGhost] = useState<{ x: number; y: number; label: string } | null>(null);
  const colRefs = useRef<(HTMLTableCellElement | null)[]>([]);

  const displayCols = dragCols ?? columns;
  const isDraggingCol = dragColIdx !== null;
  const isDraggingRow = activeRowId !== null;
  const isDragging = isDraggingCol || isDraggingRow;

  // ── Style classes ────────────────────────────────────────────────────────
  const inputClass = `w-full px-2 py-1 text-[0.8em] font-mono border rounded focus:outline-none ${c(
    "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
    "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
  )}`;
  const headerInputClass = `w-full px-2 py-1 text-[0.8em] font-mono font-medium border rounded focus:outline-none ${c(
    "bg-ink-surface/80 border-ink-border text-copper placeholder:text-text-muted focus:border-copper-dim",
    "bg-light-surface/80 border-light-border text-copper placeholder:text-light-text-muted focus:border-copper",
  )}`;
  const inputErrorClass = `w-full px-2 py-1 text-[0.8em] font-mono rounded focus:outline-none border-2 ${c(
    "bg-ink-surface border-severity-error/60 text-text-bright placeholder:text-text-muted focus:border-severity-error",
    "bg-light-surface border-severity-error/60 text-light-text-bright placeholder:text-light-text-muted focus:border-severity-error",
  )}`;
  const deleteClass = c("text-text-muted hover:text-severity-error transition-colors", "text-light-text-muted hover:text-severity-error transition-colors");
  const addClass = c("text-text-muted hover:text-copper transition-colors", "text-light-text-muted hover:text-copper transition-colors");
  const rowBorderClass = c("border-ink-border-subtle", "border-light-border-subtle");
  const overlayClass = c(
    "bg-ink-raised border-copper/60 shadow-black/60",
    "bg-white border-copper/60 shadow-black/20",
  );
  const keyClass = c(
    "text-copper border-copper/30 bg-ink-surface/80",
    "text-copper border-copper/30 bg-light-surface/80",
  );
  const valClass = c(
    "text-text-bright border-ink-border bg-ink-surface/80",
    "text-light-text-bright border-light-border bg-light-surface/80",
  );

  // ── Column ops ───────────────────────────────────────────────────────────
  const renameColumn = (ci: number, newName: string) => {
    if (!onColumnsChange || !onRowsChange) return;
    const oldName = columns[ci] ?? "";
    const next = [...columns];
    next[ci] = newName;
    onColumnsChange(next);
    if (oldName && oldName !== newName) {
      onRowsChange(
        rows.map((r) => {
          const cp = { ...r };
          if (oldName in cp) {
            cp[newName] = cp[oldName] ?? "";
            delete cp[oldName];
          }
          return cp;
        }),
      );
    }
  };

  const addColumn = () => onColumnsChange?.([...columns, ""]);

  const removeColumn = (ci: number) => {
    if (!onColumnsChange) return;
    const next = columns.filter((_, j) => j !== ci);
    if (next.length === 0) return;
    const nm = columns[ci] ?? "";
    onColumnsChange(next);
    if (nm) {
      onRowsChange?.(
        rows.map((r) => {
          const cp = { ...r };
          delete cp[nm];
          return cp;
        }),
      );
    }
  };

  // ── Row ops ──────────────────────────────────────────────────────────────
  const emptyRow = () => {
    const e: Record<string, string> = {};
    for (const col of columns) {
      if (col) e[col] = "";
    }
    return e;
  };

  const insertRowAfter = (rowId: string) => {
    const idx = ids.indexOf(rowId);
    if (idx === -1) return;
    const next = [...rows];
    next.splice(idx + 1, 0, emptyRow());
    onRowsChange?.(next);
  };

  const deleteRow = (rowId: string) => {
    const idx = ids.indexOf(rowId);
    if (idx === -1) return;
    stableIds.current.splice(idx, 1);
    onRowsChange?.(rows.filter((_, j) => j !== idx));
  };

  const updateCell = (rowId: string, col: string, val: string) => {
    if (!onRowsChange) return;
    const idx = ids.indexOf(rowId);
    if (idx === -1) return;
    const n = [...rows];
    n[idx] = { ...n[idx], [col]: val };
    onRowsChange(n);
  };

  // ── Column drag handlers ─────────────────────────────────────────────────
  const colDown = (ci: number, e: React.PointerEvent) => {
    e.preventDefault();
    setDragColIdx(ci);
    setDragCols([...columns]);
    setColGhost({ x: e.clientX, y: e.clientY, label: columns[ci] ?? "" });
    (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
  };

  const colMove = (e: React.PointerEvent) => {
    if (dragColIdx === null || !dragCols) return;
    setColGhost((g) => (g ? { ...g, x: e.clientX, y: e.clientY } : null));
    for (let i = 0; i < colRefs.current.length; i++) {
      const el = colRefs.current[i];
      if (!el || i === dragColIdx) continue;
      const r = el.getBoundingClientRect();
      const mid = r.left + r.width / 2;
      if ((dragColIdx < i && e.clientX > mid) || (dragColIdx > i && e.clientX < mid)) {
        setDragCols(arrayMove(dragCols, dragColIdx, i));
        setDragColIdx(i);
        break;
      }
    }
  };

  const colEnd = () => {
    if (dragCols) onColumnsChange?.(dragCols);
    setDragCols(null);
    setDragColIdx(null);
    setColGhost(null);
  };

  // ── Read-only ────────────────────────────────────────────────────────────
  if (readOnly) {
    // Pre-build entries so the JSX .map() below has no index parameter (avoids S6479).
    const roRows = rows.map((row, i) => ({ row, rk: `r${i}` }));
    return (
      <div className="overflow-x-auto">
        <table className="w-full text-[0.75em]">
          <thead>
            <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
              {columns.map((col, ci) => (
                <th
                  key={col || `col-${ci}`}
                  className={`px-2.5 py-1.5 text-left font-mono font-medium whitespace-nowrap ${
                    ci === keyColumnIndex ? "text-copper" : c("text-text-muted", "text-light-text-muted")
                  }`}
                >
                  {col}
                  {ci === keyColumnIndex && <KeyIcon className="inline ml-1 text-copper/60" />}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {roRows.map(({ row, rk }) => (
              <tr key={rk} className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
                {columns.map((col, ci) => (
                  <td
                    key={col || `col-${ci}`}
                    className={`px-2.5 py-1 font-mono max-w-xs truncate ${c("text-text-bright", "text-light-text-bright")}`}
                  >
                    {col ? (row[col] ?? "") : ""}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // ── Validation ────────────────────────────────────────────────────────────
  const { emptyKeyIds, duplicateKeyIds } = validateKeys(columns[keyColumnIndex] ?? "", rows, ids);

  // ── Edit mode ────────────────────────────────────────────────────────────
  // Pre-build entries so the JSX .map() below has no index parameter (avoids S6479).
  // rows[i]! — noUncheckedIndexedAccess makes this T|undefined; ids.length===rows.length is invariant. NOSONAR
  const sortableEntries = ids.map((id, i) => ({ id, row: rows[i]! })); // NOSONAR
  const activeRow = activeRowIdx >= 0 ? rows[activeRowIdx] : null;

  return (
    <div className={`rounded-lg border overflow-hidden ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
      <DndContext
        sensors={sensors}
        collisionDetection={closestCenter}
        onDragStart={handleDragStart}
        onDragEnd={handleDragEnd}
      >
        <div className="overflow-x-auto" onPointerMove={colMove} onPointerUp={colEnd}>
          <table className="w-full border-collapse">
            <thead>
              <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
                {displayCols.map((col, ci) => (
                  <th
                    key={col || `col-${ci}`}
                    ref={(el) => { colRefs.current[ci] = el; }}
                    className={`px-1 py-1.5 ${dragColIdx === ci ? "opacity-30" : ""}`}
                  >
                    <div className="flex items-center gap-0.5">
                      {/* Column grip — drag initiator for columns */}
                      <div
                        onPointerDown={(e) => colDown(ci, e)}
                        className="shrink-0 cursor-grab active:cursor-grabbing px-0.5 touch-none text-text-muted hover:text-copper transition-colors"
                        title="Drag to reorder column"
                      >
                        <GripDots />
                      </div>
                      <div className="relative flex-1 min-w-0">
                        <input
                          type="text"
                          value={col}
                          onChange={(e) => renameColumn(ci, e.target.value)}
                          placeholder={ci === keyColumnIndex ? "key" : "column"}
                          className={`w-full cursor-text ${ci === keyColumnIndex ? "pr-6 " : ""}${headerInputClass}`}
                          disabled={isDragging}
                        />
                        {ci === keyColumnIndex && (
                          <KeyIcon className="absolute right-1.5 top-1/2 -translate-y-1/2 text-copper/60 pointer-events-none" />
                        )}
                      </div>
                      {displayCols.length > 1 && (
                        <button
                          onClick={() => removeColumn(ci)}
                          className={`shrink-0 p-1.5 rounded text-base font-medium leading-none ${deleteClass}`}
                          title="Remove column"
                        >
                          ×
                        </button>
                      )}
                    </div>
                  </th>
                ))}
                {/* Spacer aligns with row delete column */}
                <th className="w-7" />
                {/* Add-row button aligns with each row's add-column cell */}
                <th className="w-7 pr-1.5 text-center">
                  <button
                    onClick={addColumn}
                    className={`p-1.5 rounded text-base font-medium leading-none ${addClass}`}
                    title="Add column"
                  >
                    +
                  </button>
                </th>
              </tr>
            </thead>
            <SortableContext items={ids} strategy={verticalListSortingStrategy}>
              <tbody>
                {sortableEntries.map(({ id, row }) => {
                  let keyError: "empty" | "duplicate" | null = null;
                  if (emptyKeyIds.has(id)) keyError = "empty";
                  else if (duplicateKeyIds.has(id)) keyError = "duplicate";
                  return (
                  <SortableRow
                    key={id}
                    id={id}
                    row={row}
                    columns={displayCols}
                    keyColumnIndex={keyColumnIndex}
                    keyError={keyError}
                    inputClass={inputClass}
                    inputErrorClass={inputErrorClass}
                    inputsDisabled={isDragging}
                    onDelete={() => deleteRow(id)}
                    onAddRow={() => insertRowAfter(id)}
                    onCellChange={(col, val) => updateCell(id, col, val)}
                    deleteClass={deleteClass}
                    addClass={addClass}
                    borderClass={rowBorderClass}
                  />
                  );
                })}
              </tbody>
            </SortableContext>
          </table>
        </div>

        {/* Row drag ghost — renders via portal to document.body */}
        <DragOverlay dropAnimation={{ duration: 200, easing: "ease" }}>
          {activeRowId && activeRow ? (
            <RowOverlay
              row={activeRow}
              columns={displayCols}
              overlayClass={overlayClass}
              keyClass={keyClass}
              valClass={valClass}
            />
          ) : null}
        </DragOverlay>
      </DndContext>

      {(emptyKeyIds.size > 0 || duplicateKeyIds.size > 0) && (
        <div className="px-3 py-1.5 text-[0.8em]">
          {emptyKeyIds.size > 0 && <span className="text-severity-error">Key column has empty values.</span>}
          {emptyKeyIds.size > 0 && duplicateKeyIds.size > 0 && <span className={c("text-text-muted", "text-light-text-muted")}> · </span>}
          {duplicateKeyIds.size > 0 && <span className="text-severity-warn">Duplicate key values.</span>}
        </div>
      )}

      {/* Column drag ghost — fixed portal follows the pointer */}
      {colGhost && createPortal(
        <div
          style={{
            position: "fixed",
            left: colGhost.x + 10,
            top: colGhost.y - 16,
            pointerEvents: "none",
            zIndex: 9999,
          }}
          className={`flex items-center gap-1.5 px-2.5 py-1.5 text-[0.8em] font-mono font-medium rounded-lg border-2 shadow-2xl scale-[1.02] whitespace-nowrap ${c(
            "bg-ink-raised border-copper/60 text-copper shadow-black/60",
            "bg-white border-copper/60 text-copper shadow-black/20",
          )}`}
        >
          <GripDots className="opacity-70" />
          {colGhost.label || <span className="opacity-40 italic">column</span>}
        </div>,
        document.body,
      )}
    </div>
  );
}
