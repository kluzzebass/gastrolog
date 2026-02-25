import { forwardRef } from "react";
import type { ProtoRecord } from "../utils";
import { clickableProps } from "../utils";
import { syntaxHighlight, composeWithSearch, type HighlightMode } from "../syntax";
import { CopyButton } from "./CopyButton";

interface SeverityInfo {
  level: string;
  label: string;
  cls: string;
  filter: string;
}

const BADGE_STYLE: Record<string, { label: string; cls: string }> = {
  error: { label: "ERR", cls: "border-severity-error/50 text-severity-error" },
  warn: { label: "WRN", cls: "border-severity-warn/50 text-severity-warn" },
  info: { label: "INF", cls: "border-severity-info/50 text-severity-info" },
  debug: { label: "DBG", cls: "border-severity-debug/50 text-severity-debug" },
  trace: { label: "TRC", cls: "border-severity-trace/50 text-severity-trace" },
};

function classifySeverity(val: string): string | null {
  if (/^(error|err|fatal|critical|emerg|alert)$/i.test(val)) return "error";
  if (/^(warn|warning)$/i.test(val)) return "warn";
  if (/^(info|notice|informational)$/i.test(val)) return "info";
  if (/^debug$/i.test(val)) return "debug";
  if (/^trace$/i.test(val)) return "trace";
  return null;
}

export function detectSeverity(
  attrs: Record<string, string>,
): SeverityInfo | null {
  for (const key of ["level", "severity", "severity_name"] as const) {
    const val = attrs[key];
    if (val) {
      const level = classifySeverity(val);
      if (level) {
        const style = BADGE_STYLE[level]!;
        return { level, ...style, filter: `${key}=${val}` };
      }
    }
  }
  return null;
}

function entryRowCls(isSelected: boolean, dark: boolean): string {
  if (isSelected) return dark ? "bg-ink-hover" : "bg-light-hover";
  return dark
    ? "hover:bg-ink-surface border-b-ink-border-subtle"
    : "hover:bg-light-hover border-b-light-border-subtle";
}

function searchHitCls(dark: boolean): string {
  return dark
    ? "bg-highlight-bg border border-highlight-border text-highlight-text px-0.5 rounded-sm"
    : "bg-light-highlight-bg border border-light-highlight-border text-light-highlight-text px-0.5 rounded-sm";
}

export const LogEntry = forwardRef<
  HTMLElement,
  {
    record: ProtoRecord;
    tokens: string[];
    isSelected: boolean;
    onSelect: () => void;
    onFilterToggle?: (token: string) => void;
    onSpanClick?: (value: string, shiftKey: boolean) => void;
    dark: boolean;
    highlightMode?: HighlightMode;
  }
>(function LogEntry(
  { record, tokens, isSelected, onSelect, onFilterToggle, onSpanClick, dark, highlightMode = "full" },
  ref,
) {
  const rawText = new TextDecoder().decode(record.raw);
  const parts = composeWithSearch(syntaxHighlight(rawText, highlightMode), tokens);
  const severity = detectSeverity(record.attrs);
  const ingestTime = record.ingestTs ? record.ingestTs.toDate() : new Date();

  const ts = ingestTime.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
    hour12: false,
  });

  return (
    <article
      ref={ref}
      onClick={onSelect}
      {...clickableProps(onSelect)}
      className={`group grid grid-cols-[3.5ch_1fr_auto] lg:grid-cols-[10ch_3.5ch_1fr_auto] px-4 lg:px-5 py-2 border-b cursor-pointer transition-colors duration-100 ${
        entryRowCls(isSelected, dark)
      }`}
    >
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
          const el = (e.target as HTMLElement).closest<HTMLElement>("[data-click-value]");
          if (el) {
            e.stopPropagation();
            onSpanClick(el.dataset.clickValue!, e.shiftKey);
          }
        } : undefined}
      >
        {parts.map((part, i) => {
          const className = part.searchHit ? searchHitCls(dark) : "";
          const style = part.color ? { color: part.color } : undefined;
          if (part.url) {
            return (
              <a
                key={`p-${i}`}
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
                key={`p-${i}`}
                style={style}
                className={`cursor-pointer hover:brightness-125 ${className}`}
                data-click-value={part.clickValue}
              >
                {part.text}
              </span>
            );
          }
          return (
            <span key={`p-${i}`} style={style} className={className}>
              {part.text}
            </span>
          );
        })}
      </div>
      <span className="self-center pl-2 opacity-0 group-hover:opacity-100 transition-opacity">
        <CopyButton text={rawText} dark={dark} />
      </span>
    </article>
  );
});
