import type { ProtoRecord } from "../utils";
import { syntaxHighlight, composeWithSearch } from "../syntax";

const RE_SEVERITY =
  /\b(ERROR|ERR|FATAL|CRITICAL|WARN(?:ING)?|INFO|NOTICE|DEBUG|TRACE)\b/i;

type Severity = "error" | "warn" | "info" | "debug" | "trace" | null;

const SEVERITY_BADGE: Record<string, { label: string; cls: string }> = {
  error: { label: "ERR", cls: "border-severity-error/50 text-severity-error" },
  warn: { label: "WRN", cls: "border-severity-warn/50 text-severity-warn" },
  info: { label: "INF", cls: "border-severity-info/50 text-severity-info" },
  debug: { label: "DBG", cls: "border-severity-debug/50 text-severity-debug" },
  trace: { label: "TRC", cls: "border-severity-trace/50 text-severity-trace" },
};

function detectSeverity(attrs: Record<string, string>, raw: string): Severity {
  // 1. Check attrs: level, severity, severity_name.
  const attrVal = (
    attrs.level ??
    attrs.severity ??
    attrs.severity_name ??
    ""
  ).toLowerCase();
  if (attrVal) {
    if (/^(error|err|fatal|critical|emerg|alert)$/.test(attrVal))
      return "error";
    if (/^(warn|warning)$/.test(attrVal)) return "warn";
    if (/^(info|notice|informational)$/.test(attrVal)) return "info";
    if (/^debug$/.test(attrVal)) return "debug";
    if (/^trace$/.test(attrVal)) return "trace";
  }
  // 2. Fall back to keyword in raw text.
  const m = RE_SEVERITY.exec(raw);
  if (!m) return null;
  const w = m[1]!.toUpperCase();
  if (w === "ERROR" || w === "ERR" || w === "FATAL" || w === "CRITICAL")
    return "error";
  if (w === "WARN" || w === "WARNING") return "warn";
  if (w === "INFO" || w === "NOTICE") return "info";
  if (w === "DEBUG") return "debug";
  return "trace";
}

export function LogEntry({
  record,
  tokens,
  isSelected,
  onSelect,
  dark,
}: {
  record: ProtoRecord;
  tokens: string[];
  isSelected: boolean;
  onSelect: () => void;
  dark: boolean;
}) {
  const rawText = new TextDecoder().decode(record.raw);
  const parts = composeWithSearch(syntaxHighlight(rawText), tokens);
  const severity = detectSeverity(record.attrs, rawText);
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
      onClick={onSelect}
      className={`grid grid-cols-[10ch_3.5ch_1fr] px-5 py-1.5 border-b cursor-pointer transition-colors duration-100 ${
        isSelected
          ? dark
            ? "bg-ink-hover"
            : "bg-light-hover"
          : dark
            ? "hover:bg-ink-surface border-b-ink-border-subtle"
            : "hover:bg-light-hover border-b-light-border-subtle"
      }`}
    >
      <span
        className={`font-mono text-[0.8em] tabular-nums self-center ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {ts}
      </span>
      <span className="self-center flex justify-center">
        {severity && (
          <span
            className={`text-[0.6em] font-semibold leading-none border rounded-sm px-0.5 py-px ${SEVERITY_BADGE[severity]!.cls}`}
          >
            {SEVERITY_BADGE[severity]!.label}
          </span>
        )}
      </span>
      <div
        className={`font-mono text-[0.85em] leading-relaxed truncate self-center pl-1.5 ${dark ? "text-text-normal" : "text-light-text-normal"}`}
      >
        {parts.map((part, i) => {
          const className = part.searchHit
            ? dark
              ? "bg-highlight-bg border border-highlight-border text-highlight-text px-0.5 rounded-sm"
              : "bg-light-highlight-bg border border-light-highlight-border text-light-highlight-text px-0.5 rounded-sm"
            : "";
          const style = part.color ? { color: part.color } : undefined;
          return part.url ? (
            <a
              key={i}
              href={part.url}
              target="_blank"
              rel="noopener noreferrer"
              style={style}
              className={`underline decoration-current/30 hover:decoration-current/60 ${className}`}
              onClick={(e) => e.stopPropagation()}
            >
              {part.text}
            </a>
          ) : (
            <span key={i} style={style} className={className}>
              {part.text}
            </span>
          );
        })}
      </div>
    </article>
  );
}
