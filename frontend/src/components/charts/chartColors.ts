/** Color cycle for multi-series line charts — CSS variables matching the severity palette. */
export const SERIES_COLORS = [
  "var(--color-copper)",
  "var(--color-severity-error)",
  "var(--color-severity-warn)",
  "var(--color-severity-info)",
  "var(--color-severity-debug)",
  "var(--color-severity-trace)",
];

/** Known severity levels mapped to their theme CSS variable colors. */
export const SEVERITY_COLOR_MAP: Record<string, string> = {
  error: "var(--color-severity-error)",
  warn: "var(--color-severity-warn)",
  info: "var(--color-severity-info)",
  debug: "var(--color-severity-debug)",
  trace: "var(--color-severity-trace)",
};

/** OKLch palette for arbitrary (non-severity) group values. */
export const GROUP_PALETTE = [
  "oklch(0.72 0.15 45)",   // copper-ish
  "oklch(0.72 0.15 160)",  // teal
  "oklch(0.72 0.15 270)",  // violet
  "oklch(0.72 0.15 90)",   // olive
  "oklch(0.72 0.15 330)",  // pink
  "oklch(0.72 0.15 210)",  // cyan
  "oklch(0.72 0.15 120)",  // green
  "oklch(0.72 0.15 20)",   // orange
];

/** Returns a color for a category name: severity colors for known levels, palette color otherwise. */
export function getColorForCategory(name: string, index: number): string {
  const severity = SEVERITY_COLOR_MAP[name.toLowerCase()];
  if (severity) return severity;
  return GROUP_PALETTE[index % GROUP_PALETTE.length]!;
}
