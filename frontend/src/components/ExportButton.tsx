import { useState, useRef } from "react";
import { useClickOutside } from "../hooks/useClickOutside";
import { useThemeClass } from "../hooks/useThemeClass";
import { DownloadIcon } from "./icons";
import type { ProtoRecord } from "../utils";

type Format = "json" | "csv";

function recordToPlain(record: ProtoRecord) {
  const raw = new TextDecoder().decode(record.raw);
  return {
    timestamp: record.ingestTs ? record.ingestTs.toDate().toISOString() : "",
    sourceTimestamp: record.sourceTs
      ? record.sourceTs.toDate().toISOString()
      : "",
    message: raw,
    ...record.attrs,
  };
}

function escapeCSV(val: string): string {
  if (val.includes('"') || val.includes(",") || val.includes("\n")) {
    return `"${val.replace(/"/g, '""')}"`;
  }
  return val;
}

function toJSON(records: ProtoRecord[]): string {
  return JSON.stringify(records.map(recordToPlain), null, 2);
}

function toCSV(records: ProtoRecord[]): string {
  if (records.length === 0) return "";
  const rows = records.map(recordToPlain);
  // Collect all keys across all rows for consistent columns.
  const keys = Array.from(new Set(rows.flatMap(Object.keys)));
  const header = keys.map(escapeCSV).join(",");
  const lines = rows.map((row) =>
    keys
      .map((k) => {
        const val = (row as Record<string, unknown>)[k];
        if (val == null) return escapeCSV("");
        if (typeof val === "object") return escapeCSV(JSON.stringify(val));
        return escapeCSV(`${val as string | number | boolean}`);
      })
      .join(","),
  );
  return [header, ...lines].join("\n");
}

function download(content: string, filename: string, mime: string) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

interface TableData {
  columns: string[];
  rows: string[][];
}

function tableToJSON(table: TableData): string {
  const objs = table.rows.map((row) =>
    Object.fromEntries(table.columns.map((col, i) => [col, row[i] ?? ""])),
  );
  return JSON.stringify(objs, null, 2);
}

function tableToCSV(table: TableData): string {
  const header = table.columns.map(escapeCSV).join(",");
  const lines = table.rows.map((row) => row.map(escapeCSV).join(","));
  return [header, ...lines].join("\n");
}

export function ExportButton({
  records,
  tableData,
  dark,
}: Readonly<{
  records?: ProtoRecord[];
  tableData?: TableData;
  dark: boolean;
}>) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useClickOutside(
    ref,
    () => setOpen(false),
  );

  const isEmpty = tableData
    ? tableData.rows.length === 0
    : !records || records.length === 0;

  const handleExport = (format: Format) => {
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    if (tableData) {
      if (format === "json") {
        download(tableToJSON(tableData), `gastrolog-${ts}.json`, "application/json");
      } else {
        download(tableToCSV(tableData), `gastrolog-${ts}.csv`, "text/csv");
      }
    } else if (records) {
      if (format === "json") {
        download(toJSON(records), `gastrolog-${ts}.json`, "application/json");
      } else {
        download(toCSV(records), `gastrolog-${ts}.csv`, "text/csv");
      }
    }
    setOpen(false);
  };

  const c = useThemeClass(dark);

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen((o) => !o)}
        disabled={isEmpty}
        aria-label="Export results"
        title="Export results"
        className={`w-8 h-8 flex items-center justify-center rounded transition-colors disabled:opacity-30 disabled:cursor-not-allowed ${c(
          "text-text-muted hover:text-copper hover:bg-ink-hover",
          "text-light-text-muted hover:text-copper hover:bg-light-hover",
        )}`}
      >
        <DownloadIcon className="w-4 h-4" />
      </button>
      {open && (
        <div
          className={`absolute right-0 top-full mt-1 z-40 rounded border shadow-lg min-w-28 ${c("bg-ink-surface border-ink-border", "bg-light-surface border-light-border")}`}
        >
          {(["json", "csv"] as Format[]).map((fmt) => (
            <button
              key={fmt}
              onClick={() => handleExport(fmt)}
              className={`block w-full text-left px-3 py-2.5 text-[0.8em] font-mono transition-colors ${c("text-text-muted hover:text-copper hover:bg-ink-hover", "text-light-text-muted hover:text-copper hover:bg-light-hover")}`}
            >
              Export as {fmt.toUpperCase()}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
