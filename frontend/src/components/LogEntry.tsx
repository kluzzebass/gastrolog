import { forwardRef } from "react";
import type { ProtoRecord } from "../utils";
import { clickableProps } from "../utils";
import { protoToInstant, formatLocalTime } from "../utils/temporal";
import { syntaxHighlight, composeWithSearch, type HighlightMode } from "../syntax";
import { CopyButton } from "./CopyButton";
import { SEVERITIES, classifySeverity } from "../lib/severity";

interface SeverityInfo {
  level: string;
  label: string;
  cls: string;
  filter: string;
}

export function detectSeverity(
  attrs: Record<string, string>,
): SeverityInfo | null {
  for (const key of ["level", "severity", "severity_name"] as const) {
    const val = attrs[key];
    if (val) {
      const level = classifySeverity(val);
      if (level) {
        const sev = SEVERITIES[level];
        return { level, label: sev.short, cls: sev.badgeCls, filter: `${key}=${val}` };
      }
    }
  }
  return null;
}

function entryRowCls(isSelected: boolean, dark: boolean): string {
  if (isSelected) return dark ? "bg-ink-hover border-b-ink-border-subtle" : "bg-light-hover border-b-light-border-subtle";
  return dark
    ? "hover:bg-ink-surface border-b-ink-border-subtle"
    : "hover:bg-light-hover border-b-light-border-subtle";
}

function searchHitCls(dark: boolean): string {
  return dark
    ? "bg-highlight-bg border border-highlight-border text-highlight-text px-0.5 rounded-sm"
    : "bg-light-highlight-bg border border-light-highlight-border text-light-highlight-text px-0.5 rounded-sm";
}

export type OrderByTS = "ingest_ts" | "source_ts" | "write_ts";

/** Extract the `order=` directive value from a query string. */
export function parseOrderBy(query: string): OrderByTS {
  const m = /\border=(\w+)/.exec(query);
  if (m && (m[1] === "source_ts" || m[1] === "write_ts")) return m[1];
  return "ingest_ts";
}

function pickTS(record: ProtoRecord, orderBy: OrderByTS) {
  switch (orderBy) {
    case "source_ts": return record.sourceTs;
    case "write_ts":  return record.writeTs;
    default:          return record.ingestTs;
  }
}

export const LogEntry = forwardRef<
  HTMLElement,
  {
    record: ProtoRecord;
    tokens: string[];
    isSelected: boolean;
    onSelect: () => void;
    onFilterToggle?: (token: string) => void;
    onSpanClick?: (value: string) => void;
    dark: boolean;
    highlightMode?: HighlightMode;
    orderBy?: OrderByTS;
    rowIndex?: number;
  }
>(function LogEntry(
  { record, tokens, isSelected, onSelect, onFilterToggle, onSpanClick, dark, highlightMode = "full", orderBy = "ingest_ts", rowIndex },
  ref,
) {
  const rawText = new TextDecoder().decode(record.raw).trimEnd();
  const parts = composeWithSearch(syntaxHighlight(rawText, highlightMode), tokens);
  const severity = detectSeverity(record.attrs);
  const tsProto = pickTS(record, orderBy);
  const tsInstant = tsProto ? protoToInstant(tsProto) : null;
  const ts = tsInstant ? formatLocalTime(tsInstant) : "--:--:--";

  return (
    <article
      ref={ref}
      onClick={onSelect}
      className={`group grid grid-cols-[3.5ch_1fr_auto] ${
        rowIndex != null
          ? "lg:grid-cols-[5ch_10ch_3.5ch_1fr_auto]"
          : "lg:grid-cols-[10ch_3.5ch_1fr_auto]"
      } px-4 lg:px-5 py-2 border-b cursor-pointer transition-colors duration-100 ${
        entryRowCls(isSelected, dark)
      }`}
    >
      {rowIndex != null && (
        <span
          className={`hidden lg:block font-mono text-[0.65em] tabular-nums self-center text-right pr-1 ${dark ? "text-text-ghost/50" : "text-light-text-ghost/50"}`}
        >
          {rowIndex}
        </span>
      )}
      <span
        className={`hidden lg:block font-mono text-[0.8em] tabular-nums self-center ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {ts}
      </span>
      <span className="self-center flex justify-center">
        {severity && (
          <span
            className={`text-[0.6em] font-semibold leading-none border rounded-sm px-0.5 py-px cursor-pointer hover:brightness-125 transition-[filter] ${severity.cls}`}
            onClick={(e) => {
              e.stopPropagation();
              onFilterToggle?.(severity.filter);
            }}
            {...clickableProps(onFilterToggle ? () => onFilterToggle(severity.filter) : undefined)}
            aria-label={`Filter by ${severity.label}`}
          >
            {severity.label}
          </span>
        )}
      </span>
      <div
        className={`font-mono text-[0.85em] leading-relaxed truncate whitespace-pre self-center pl-1.5 ${dark ? "text-text-normal" : "text-light-text-normal"}`}
        onClick={onSpanClick ? (e) => {
          if (!e.altKey) return; // plain click bubbles to onSelect
          const el = (e.target as HTMLElement).closest<HTMLElement>("[data-click-value]");
          if (el) {
            e.stopPropagation();
            onSpanClick(el.dataset.clickValue!);
          }
        } : undefined}
      >
        {(() => {
          const offsets: number[] = [];
          for (let j = 0; j < parts.length; j++) {
            offsets.push(j === 0 ? 0 : offsets[j - 1]! + parts[j - 1]!.text.length);
          }
          return parts.map((part, i) => {
            const className = part.searchHit ? searchHitCls(dark) : "";
            const style = part.color ? { color: part.color } : undefined;
            const key = `p-${offsets[i]}`;
            if (part.url) {
              return (
                <a
                  key={key}
                  href={part.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={style}
                  className={`underline decoration-current/30 hover:decoration-current/60 ${className}`}
                  onClick={(e) => e.stopPropagation()}
                >
                  {part.text}
                </a>
              );
            }
            if (part.clickValue) {
              return (
                <span
                  key={key}
                  style={style}
                  className={`cursor-pointer hover:brightness-125 ${className}`}
                  data-click-value={part.clickValue}
                  title="⌥ click to add filter"
                >
                  {part.text}
                </span>
              );
            }
            return (
              <span key={key} style={style} className={className}>
                {part.text}
              </span>
            );
          });
        })()}
      </div>
      <span className="self-center pl-2 opacity-0 group-hover:opacity-100 transition-opacity">
        <CopyButton text={rawText} dark={dark} />
      </span>
    </article>
  );
});
