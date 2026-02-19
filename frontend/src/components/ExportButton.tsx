import { useState, useCallback, useRef } from "react";
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
      .map((k) => escapeCSV(String((row as Record<string, unknown>)[k] ?? "")))
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

export function ExportButton({
  records,
  dark,
}: Readonly<{
  records: ProtoRecord[];
  dark: boolean;
}>) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useClickOutside(
    ref,
    useCallback(() => setOpen(false), []),
  );

  const handleExport = useCallback(
    (format: Format) => {
      const ts = new Date().toISOString().replace(/[:.]/g, "-");
      if (format === "json") {
        download(toJSON(records), `gastrolog-${ts}.json`, "application/json");
      } else {
        download(toCSV(records), `gastrolog-${ts}.csv`, "text/csv");
      }
      setOpen(false);
    },
    [records],
  );

  const c = useThemeClass(dark);

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen((o) => !o)}
        disabled={records.length === 0}
        aria-label="Export results"
        title="Export results"
        className={`w-6 h-6 flex items-center justify-center rounded transition-colors disabled:opacity-30 disabled:cursor-not-allowed ${c(
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
              className={`block w-full text-left px-3 py-1.5 text-[0.8em] font-mono transition-colors ${c("text-text-muted hover:text-copper hover:bg-ink-hover", "text-light-text-muted hover:text-copper hover:bg-light-hover")}`}
            >
              Export as {fmt.toUpperCase()}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
