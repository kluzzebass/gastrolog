/** Schemes a rendered help link may keep in its `href`. */
const SAFE_SCHEMES = new Set(["http", "https", "mailto"]);

/**
 * App-internal pseudo-schemes. `helpMarkdownComponents` turns these into
 * buttons rather than anchors, so they never reach the DOM as an `href`, but
 * the transform runs first and must not strip them.
 */
const INTERNAL_SCHEMES = new Set(["help", "settings", "inspector"]);

const SCHEME = /^([a-z][a-z0-9+.-]*):/i;

/**
 * Restrict Markdown link and image URLs to relative paths, the schemes in
 * `SAFE_SCHEMES`, and the app's own pseudo-schemes. Everything else — most
 * importantly `javascript:` and `data:` — collapses to an empty string.
 *
 * Browsers ignore ASCII whitespace and control characters inside a URL, so the
 * scheme is detected on a stripped copy while the original value is what gets
 * returned.
 */
export function markdownUrlTransform(url: string): string {
  const probe = url.replace(/[\s\p{Cc}]/gu, "");
  const beforeDelimiter = probe.split(/[/?#]/, 1)[0] ?? "";
  const scheme = SCHEME.exec(beforeDelimiter)?.[1]?.toLowerCase();
  if (scheme === undefined) return url; // relative URL or bare fragment
  if (INTERNAL_SCHEMES.has(scheme) || SAFE_SCHEMES.has(scheme)) return url;
  return "";
}
