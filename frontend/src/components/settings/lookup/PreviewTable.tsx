import { useThemeClass } from "../../../hooks/useThemeClass";

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function cellString(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v;
  if (typeof v === "number") return String(v);
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "bigint") return v.toString(10);
  if (typeof v === "symbol") return v.description ?? v.toString();
  if (typeof v === "function") return v.name ? `[function ${v.name}]` : "[function]";
  return JSON.stringify(v);
}

function unwrapJqSingleArrayEmit(items: unknown[]): unknown[] {
  if (
    items.length === 1 &&
    Array.isArray(items[0]) &&
    items[0].every((e) => isPlainObject(e))
  ) {
    return items[0];
  }
  return items;
}

function parseArrayOfArrays(items: unknown[]): { columns: string[]; rows: string[][] } | null {
  const first = items[0];
  if (!Array.isArray(first)) return null;
  const columns = first.map(cellString);
  if (columns.length === 0) return null;
  const rows: string[][] = [];
  for (let i = 1; i < items.length; i++) {
    const row = items[i];
    if (!Array.isArray(row)) return null;
    rows.push(row.map(cellString));
  }
  return { columns, rows };
}

function parseArrayOfObjects(items: unknown[]): { columns: string[]; rows: string[][] } | null {
  const first = items[0];
  if (!isPlainObject(first)) return null;
  const columns = Object.keys(first);
  if (columns.length === 0) return null;
  const rows: string[][] = [];
  for (const item of items) {
    if (!isPlainObject(item)) return null;
    rows.push(columns.map((col) => cellString(item[col])));
  }
  return { columns, rows };
}

/**
 * Shared preview table for CSV and JSON lookup results.
 * Renders column headers with an optional highlighted key column,
 * and sample data rows.
 */
export function PreviewTable({
  dark,
  columns,
  rows,
  keyColumn,
}: Readonly<{
  dark: boolean;
  columns: string[];
  rows: string[][];
  keyColumn?: string;
}>) {
  const c = useThemeClass(dark);
  if (columns.length === 0) return null;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-[0.75em]">
        <thead>
          <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
            {columns.map((col) => (
              <th
                key={col}
                className={`px-2.5 py-1.5 text-left font-mono font-medium whitespace-nowrap ${
                  col === keyColumn
                    ? "text-copper"
                    : c("text-text-muted", "text-light-text-muted")
                }`}
              >
                {col}
                {col === keyColumn && (
                  <span className="ml-1 text-[0.85em] opacity-60">key</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr
              key={ri}
              className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              {row.map((val, ci) => (
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
  );
}

/**
 * Parse a jq query result (JSON string) into a tabular format.
 * Accepts two shapes:
 *   1. Array of objects — keys from the first object become column headers.
 *   2. Array of arrays — first array is the header row, rest are data rows.
 * Returns null if the result doesn't match either shape.
 */
export function parseTabularResult(jsonStr: string): { columns: string[]; rows: string[][] } | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    return null;
  }

  if (!Array.isArray(parsed) || parsed.length === 0) return null;

  // Unwrap the single-value wrapper the backend adds when a jq expression
  // emits one output that is itself an array. Without this, `[.hosts[] | {...}]`
  // (bracket-wrapped, single emit) arrives as `[[{...}, {...}]]` and gets
  // mistaken for a header-row CSV shape with objects as column names.
  const items = unwrapJqSingleArrayEmit(parsed);
  return parseArrayOfArrays(items) ?? parseArrayOfObjects(items);
}
