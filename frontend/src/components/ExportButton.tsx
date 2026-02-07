import { useState, useCallback, useRef, useEffect } from "react";
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
}: {
  records: ProtoRecord[];
  dark: boolean;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

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

  // Close on click outside.
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const c = (d: string, l: string) => (dark ? d : l);

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen((o) => !o)}
        disabled={records.length === 0}
        title="Export results"
        className={`w-6 h-6 flex items-center justify-center rounded transition-colors disabled:opacity-30 disabled:cursor-not-allowed ${c(
          "text-text-muted hover:text-copper hover:bg-ink-hover",
          "text-light-text-muted hover:text-copper hover:bg-light-hover",
        )}`}
      >
        <svg
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="w-4 h-4"
        >
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
          <polyline points="7 10 12 15 17 10" />
          <line x1="12" y1="15" x2="12" y2="3" />
        </svg>
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
