/** Canonical severity levels in descending order. */
export const SEVERITY_LEVELS = ["error", "warn", "info", "debug", "trace"] as const;
type SeverityLevel = (typeof SEVERITY_LEVELS)[number];

interface SeverityDef {
  label: string;        // "Error"
  short: string;        // "ERR"
  color: string;        // "severity-error" (Tailwind class stem)
  cssVar: string;       // "var(--color-severity-error)"
  badgeCls: string;     // LogEntry badge classes
  toggleActive: string; // Sidebar toggle active state
  toggleInactive: string;
}

/** Per-level display and styling data. */
export const SEVERITIES: Record<SeverityLevel, SeverityDef> = {
  error: {
    label: "Error", short: "ERR", color: "severity-error",
    cssVar: "var(--color-severity-error)",
    badgeCls: "border-severity-error/50 text-severity-error",
    toggleActive: "bg-severity-error border-severity-error text-white",
    toggleInactive: "border-severity-error/40 text-severity-error hover:border-severity-error hover:bg-severity-error/10",
  },
  warn: {
    label: "Warn", short: "WRN", color: "severity-warn",
    cssVar: "var(--color-severity-warn)",
    badgeCls: "border-severity-warn/50 text-severity-warn",
    toggleActive: "bg-severity-warn border-severity-warn text-white",
    toggleInactive: "border-severity-warn/40 text-severity-warn hover:border-severity-warn hover:bg-severity-warn/10",
  },
  info: {
    label: "Info", short: "INF", color: "severity-info",
    cssVar: "var(--color-severity-info)",
    badgeCls: "border-severity-info/50 text-severity-info",
    toggleActive: "bg-severity-info border-severity-info text-white",
    toggleInactive: "border-severity-info/40 text-severity-info hover:border-severity-info hover:bg-severity-info/10",
  },
  debug: {
    label: "Debug", short: "DBG", color: "severity-debug",
    cssVar: "var(--color-severity-debug)",
    badgeCls: "border-severity-debug/50 text-severity-debug",
    toggleActive: "bg-severity-debug border-severity-debug text-white",
    toggleInactive: "border-severity-debug/40 text-severity-debug hover:border-severity-debug hover:bg-severity-debug/10",
  },
  trace: {
    label: "Trace", short: "TRC", color: "severity-trace",
    cssVar: "var(--color-severity-trace)",
    badgeCls: "border-severity-trace/50 text-severity-trace",
    toggleActive: "bg-severity-trace border-severity-trace text-white",
    toggleInactive: "border-severity-trace/40 text-severity-trace hover:border-severity-trace hover:bg-severity-trace/10",
  },
};

/** CSS variable color map for chart rendering (ECharts). */
export const SEVERITY_COLOR_MAP: Record<string, string> = Object.fromEntries(
  SEVERITY_LEVELS.map((l) => [l, SEVERITIES[l].cssVar]),
);

/** Normalize raw severity strings (e.g. "ERR", "fatal", "warning") to canonical levels. */
export function classifySeverity(val: string): SeverityLevel | null {
  if (/^(error|err|fatal|critical|emerg|alert)$/i.test(val)) return "error";
  if (/^(warn|warning)$/i.test(val)) return "warn";
  if (/^(info|notice|informational)$/i.test(val)) return "info";
  if (/^debug$/i.test(val)) return "debug";
  if (/^trace$/i.test(val)) return "trace";
  return null;
}
