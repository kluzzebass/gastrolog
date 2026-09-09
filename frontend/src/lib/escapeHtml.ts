const HTML_ENTITIES: Record<string, string> = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
};

/**
 * Escape a value for interpolation into an HTML string.
 *
 * Anything built as raw HTML rather than JSX must pass every interpolated
 * value through here. Chart categories, series names and column headers come
 * from log records, which any unauthenticated producer can write.
 *
 * `null` and `undefined` become the empty string so callers do not print
 * "undefined" into a tooltip.
 */
export function escapeHtml(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined) return "";
  return String(value).replace(/[&<>"']/g, (ch) => HTML_ENTITIES[ch]!);
}
