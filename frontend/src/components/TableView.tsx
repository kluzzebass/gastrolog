import { useState } from "react";
import { useThemeClass } from "../hooks/useThemeClass";

interface TableViewProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
}

type SortDir = "asc" | "desc";

export function TableView({ columns, rows, dark }: Readonly<TableViewProps>) {
  const c = useThemeClass(dark);
  const [sortCol, setSortCol] = useState<number | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const handleSort = (colIdx: number) => {
    if (sortCol === colIdx) {
      setSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortCol(colIdx);
      setSortDir("asc");
    }
  };

  // Detect numeric columns: all non-empty values parse as numbers.
  const isNumeric = columns.map((_, colIdx) =>
    rows.length > 0 &&
    rows.every((row) => {
      const v = row[colIdx];
      return v === undefined || v === "" || !isNaN(Number(v));
    }),
  );

  const sortedRows = (() => {
    if (sortCol === null) return rows;
    const col = sortCol;
    const dir = sortDir === "asc" ? 1 : -1;
    const numeric = isNumeric[col];
    return [...rows].sort((a, b) => {
      const va = a[col] ?? "";
      const vb = b[col] ?? "";
      if (numeric) {
        return (Number(va) - Number(vb)) * dir;
      }
      return va.localeCompare(vb) * dir;
    });
  })();

  return (
    <div className="overflow-auto app-scroll">
      <table className="w-full font-mono text-[0.8em] border-collapse">
        <thead>
          <tr>
            {columns.map((col, i) => {
              let ariaSort: "ascending" | "descending" | undefined;
              if (sortCol === i) ariaSort = sortDir === "asc" ? "ascending" : "descending";
              return (
              <th
                key={col}
                onClick={() => handleSort(i)}
                aria-sort={ariaSort}
                className={`text-left px-3 py-2 cursor-pointer select-none border-b whitespace-nowrap ${c(
                  "border-ink-border-subtle text-text-muted hover:text-copper",
                  "border-light-border-subtle text-light-text-muted hover:text-copper",
                )}`}
              >
                {col}
                {sortCol === i && (
                  <span className="ml-1 text-copper" aria-hidden="true">
                    {sortDir === "asc" ? "\u25B4" : "\u25BE"}
                  </span>
                )}
              </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row, rowIdx) => (
            <tr
              key={rowIdx}
              className={`transition-colors ${
                rowIdx % 2 === 0
                  ? c("bg-ink-surface", "bg-light-surface")
                  : c("bg-ink-raised", "bg-light-bg")
              } ${c("hover:bg-ink-hover", "hover:bg-light-hover")}`}
            >
              {columns.map((col, colIdx) => (
                <td
                  key={col}
                  className={`px-3 py-1.5 whitespace-nowrap ${c(
                    "text-text-bright",
                    "text-light-text-bright",
                  )} ${isNumeric[colIdx] ? "text-right tabular-nums" : ""}`}
                >
                  {row[colIdx] ?? ""}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
