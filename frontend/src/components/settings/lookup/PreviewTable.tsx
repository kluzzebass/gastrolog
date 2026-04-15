import { useThemeClass } from "../../../hooks/useThemeClass";

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

  const first = parsed[0];

  // Shape 1: Array of arrays — first row is headers.
  if (Array.isArray(first)) {
    const columns = first.map(String);
    if (columns.length === 0) return null;
    const rows: string[][] = [];
    for (let i = 1; i < parsed.length; i++) {
      const row = parsed[i];
      if (!Array.isArray(row)) return null;
      rows.push(row.map((v: unknown) => {
        if (v === null || v === undefined) return "";
        if (typeof v === "object") return JSON.stringify(v);
        return String(v);
      }));
    }
    return { columns, rows };
  }

  // Shape 2: Array of objects — keys from first object.
  if (typeof first !== "object" || first === null) return null;
  const obj = first as Record<string, unknown>;
  const columns = Object.keys(obj);
  if (columns.length === 0) return null;

  const rows: string[][] = [];
  for (const item of parsed) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) return null;
    const o = item as Record<string, unknown>;
    rows.push(columns.map((col) => {
      const v = o[col];
      if (v === null || v === undefined) return "";
      if (typeof v === "object") return JSON.stringify(v);
      return String(v);
    }));
  }

  return { columns, rows };
}
