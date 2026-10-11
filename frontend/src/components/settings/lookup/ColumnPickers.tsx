import { useThemeClass } from "../../../hooks/useThemeClass";
import { FormField, SelectInput } from "../FormField";
import { Checkbox } from "../Checkbox";

/**
 * Key and value column pickers for a file lookup. They are always on
 * screen: without a table to pick from they are disabled with the reason.
 *
 * The wire shape of valueColumns is "empty means every non-key column", and
 * an empty list follows columns added to the file later, where an explicit
 * list does not. The **All** checkbox is that distinction made visible, in
 * the same shape as the ingester node picker: when it is on, the per-column
 * boxes show as checked and disabled; turning it off writes the current
 * columns out as an explicit list to edit.
 */
export function ColumnPickers({
  columns,
  keyColumn,
  valueColumns,
  onKeyColumnChange,
  onValueColumnsChange,
  unavailableReason,
  helpRef,
  dark,
}: Readonly<{
  columns: string[];
  keyColumn: string;
  valueColumns: string[];
  onKeyColumnChange: (v: string) => void;
  onValueColumnsChange: (v: string[]) => void;
  /** Why no column can be picked right now; undefined when the table is available. */
  unavailableReason?: string;
  helpRef: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const resolvedKey = keyColumn || columns[0] || "";
  const nonKeyColumns = columns.filter((col) => col !== resolvedKey);
  const allColumns = valueColumns.length === 0;

  const toggleValueColumn = (col: string) => {
    onValueColumnsChange(
      valueColumns.includes(col) ? valueColumns.filter((v) => v !== col) : [...valueColumns, col],
    );
  };

  const keyOptions =
    columns.length > 0
      ? [
          { value: "", label: `${columns[0]} (first column)` },
          ...columns.slice(1).map((col) => ({ value: col, label: col })),
        ]
      : [{ value: "", label: "(first column)" }];

  return (
    <>
      <FormField
        label="Key Column"
        description="Column used as the lookup key."
        helpRef={helpRef}
        disabledReason={unavailableReason}
        dark={dark}
      >
        <SelectInput value={keyColumn} onChange={onKeyColumnChange} options={keyOptions} dark={dark} />
      </FormField>

      <FormField
        label="Value Columns"
        description="Columns included in lookup results."
        helpRef={helpRef}
        disabledReason={unavailableReason}
        dark={dark}
      >
        <div className={`flex flex-col gap-1.5 px-2 py-1.5 rounded border ${c(
          "border-ink-border bg-ink-surface",
          "border-light-border bg-light-surface",
        )}`}>
          <Checkbox
            checked={allColumns}
            onChange={(on) => onValueColumnsChange(on ? [] : nonKeyColumns)}
            label="All non-key columns — including columns added to the file later"
            dark={dark}
          />
          {nonKeyColumns.length > 0 && (
            <div className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`} />
          )}
          <div className="flex flex-wrap gap-x-3 gap-y-1.5">
            {nonKeyColumns.map((col) => (
              <Checkbox
                key={col}
                checked={allColumns || valueColumns.includes(col)}
                onChange={() => toggleValueColumn(col)}
                label={<span className={`text-[0.85em] font-mono ${c("text-text-bright", "text-light-text-bright")}`}>{col}</span>}
                disabled={allColumns}
                dark={dark}
              />
            ))}
          </div>
        </div>
      </FormField>
    </>
  );
}
