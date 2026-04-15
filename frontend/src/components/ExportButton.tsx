import { useState, useRef } from "react";
import { useClickOutside } from "../hooks/useClickOutside";
import { useThemeClass } from "../hooks/useThemeClass";
import { protoToInstant, instantToISO } from "../utils/temporal";
import { DownloadIcon } from "./icons";
import { queryClient, Query } from "../api/client";
import type { ProtoRecord } from "../utils";

type Format = "json" | "csv";

function recordToPlain(record: ProtoRecord) {
  const raw = new TextDecoder().decode(record.raw);
  return {
    timestamp: record.ingestTs ? instantToISO(protoToInstant(record.ingestTs)) : "",
    sourceTimestamp: record.sourceTs
      ? instantToISO(protoToInstant(record.sourceTs))
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

async function drainSearch(expression: string): Promise<ProtoRecord[]> {
  const all: ProtoRecord[] = [];
  let resumeToken = new Uint8Array(0);
  let hasMore = true;

  while (hasMore) {
    const query = new Query();
    query.expression = expression;
    query.limit = BigInt(10000);

    let lastResumeToken = new Uint8Array(0);
    let gotRecords = false;

    for await (const response of queryClient.search(
      { query, resumeToken },
    )) {
      for (const rec of response.records) {
        all.push(rec);
        gotRecords = true;
      }
      if (response.resumeToken.length > 0) {
        lastResumeToken = response.resumeToken;
      }
    }

    if (!gotRecords || lastResumeToken.length === 0) {
      hasMore = false;
    } else {
      resumeToken = lastResumeToken;
    }
  }

  return all;
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
  onExportToVault,
  queryExpression,
}: Readonly<{
  records?: ProtoRecord[];
  tableData?: TableData;
  dark: boolean;
  onExportToVault?: () => void;
  queryExpression?: string;
}>) {
  const [open, setOpen] = useState(false);
  const [exporting, setExporting] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useClickOutside(
    ref,
    () => setOpen(false),
  );

  const isEmpty = tableData
    ? tableData.rows.length === 0
    : !records || records.length === 0;

  const handleExport = async (format: Format) => {
    setOpen(false);

    // Table data exports use in-memory data (pipeline results are already complete).
    if (tableData) {
      const ts = new Date().toISOString().replace(/[:.]/g, "-");
      if (format === "json") {
        download(tableToJSON(tableData), `gastrolog-${ts}.json`, "application/json");
      } else {
        download(tableToCSV(tableData), `gastrolog-${ts}.csv`, "text/csv");
      }
      return;
    }

    // Record exports: drain the full search from the backend.
    if (queryExpression) {
      setExporting(true);
      try {
        const allRecords = await drainSearch(queryExpression);
        const ts = new Date().toISOString().replace(/[:.]/g, "-");
        if (format === "json") {
          download(toJSON(allRecords), `gastrolog-${ts}.json`, "application/json");
        } else {
          download(toCSV(allRecords), `gastrolog-${ts}.csv`, "text/csv");
        }
        setExporting(false);
      } catch {
        // drainSearch errors are non-fatal — the export just stops early.
        setExporting(false);
      }
      return;
    }

    // Fallback: use in-memory records.
    if (records) {
      const ts = new Date().toISOString().replace(/[:.]/g, "-");
      if (format === "json") {
        download(toJSON(records), `gastrolog-${ts}.json`, "application/json");
      } else {
        download(toCSV(records), `gastrolog-${ts}.csv`, "text/csv");
      }
    }
  };

  const c = useThemeClass(dark);

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen((o) => !o)}
        disabled={isEmpty || exporting}
        aria-label={exporting ? "Exporting..." : "Export results"}
        title={exporting ? "Exporting..." : "Export results"}
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
          {onExportToVault && (
            <>
              <div className={`border-t mx-2 my-1 ${c("border-ink-border-subtle", "border-light-border-subtle")}`} />
              <button
                onClick={() => {
                  setOpen(false);
                  onExportToVault();
                }}
                className={`block w-full text-left px-3 py-2.5 text-[0.8em] font-mono transition-colors ${c("text-text-muted hover:text-copper hover:bg-ink-hover", "text-light-text-muted hover:text-copper hover:bg-light-hover")}`}
              >
                Export to vault
              </button>
            </>
          )}
        </div>
      )}
    </div>
  );
}
