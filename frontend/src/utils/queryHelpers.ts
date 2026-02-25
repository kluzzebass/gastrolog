import { timeRangeMs } from "../utils";

export const stripTimeRange = (q: string): string =>
  q
    .replace(/\blast=\S+/g, "")
    .replace(/\bstart=\S+/g, "")
    .replace(/\bend=\S+/g, "")
    .replace(/\breverse=\S+/g, "")
    .replace(/[^\S\n]+/g, " ")
    .trim();

export const stripStore = (q: string): string =>
  q
    .replace(/\bstore=\S+/g, "")
    .replace(/[^\S\n]+/g, " ")
    .trim();

export const stripChunk = (q: string): string =>
  q
    .replace(/\bchunk=\S+/g, "")
    .replace(/[^\S\n]+/g, " ")
    .trim();

export const stripPos = (q: string): string =>
  q
    .replace(/\bpos=\S+/g, "")
    .replace(/[^\S\n]+/g, " ")
    .trim();

export const stripSeverity = (qs: string): string =>
  qs
    .replace(/\((?:level=\w+\s+OR\s+)*level=\w+\)/g, "")
    .replace(/\blevel=(?:error|warn|info|debug|trace)\b/g, "")
    .replace(/\bnot\s+level=\*\b/gi, "")
    .replace(/[^\S\n]+/g, " ")
    .trim();

/** Strip all directives, returning only the user's search expression. */
export const stripAllDirectives = (q: string): string =>
  q
    .replace(/\blast=\S+/g, "")
    .replace(/\bstart=\S+/g, "")
    .replace(/\bend=\S+/g, "")
    .replace(/\breverse=\S+/g, "")
    .replace(/\bstore=\S+/g, "")
    .replace(/\blimit=\S+/g, "")
    .replace(/\bchunk=\S+/g, "")
    .replace(/\bpos=\S+/g, "")
    .replace(/[^\S\n]+/g, " ")
    .trim();

export const buildTimeTokens = (range: string, reverse: boolean): string => {
  const rev = `reverse=${reverse}`;
  if (range === "All") return rev;
  if (range in timeRangeMs) return `last=${range} ${rev}`;
  return rev;
};

export const injectTimeRange = (
  q: string,
  range: string,
  reverse: boolean,
): string => {
  const base = stripTimeRange(q);
  const timeTokens = buildTimeTokens(range, reverse);
  return base ? `${timeTokens} ${base}` : timeTokens;
};

export const injectStore = (q: string, storeId: string): string => {
  const base = stripStore(q);
  if (storeId === "all") return base;
  const token = `store=${storeId}`;
  return base ? `${token} ${base}` : token;
};

/** Extract the directive tokens (last=, start=, end=, reverse=, store=, limit=, chunk=, pos=) from a query. */
export const extractDirectives = (q: string): string => {
  const directives: string[] = [];
  const re = /\b(last|start|end|reverse|store|limit|chunk|pos)=\S+/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(q)) !== null) {
    directives.push(m[0]);
  }
  return directives.join(" ");
};

/** Replace the expression part of a query with a new value, preserving directives. */
export const replaceExpression = (q: string, value: string): string => {
  const directives = extractDirectives(q);
  return directives ? `${directives} ${value}` : value;
};

/** Append a value to the expression using OR, wrapping in parens as needed. */
export const appendOrExpression = (q: string, value: string): string => {
  const directives = extractDirectives(q);
  const expr = stripAllDirectives(q);
  let newExpr: string;
  if (!expr) {
    newExpr = value;
  } else if (expr.startsWith("(") && expr.endsWith(")")) {
    // Already a group — insert before closing paren.
    newExpr = expr.slice(0, -1) + " OR " + value + ")";
  } else {
    newExpr = "(" + expr + " OR " + value + ")";
  }
  return directives ? `${directives} ${newExpr}` : newExpr;
};

/**
 * Determine which action the main search/follow effect should take.
 *
 * Follow mode is handled first — it doesn't use time ranges, so the
 * default-range injection (which navigates and could change the route)
 * must never run when following.
 */
export type QueryEffectAction = "follow" | "search" | "inject-default-range" | "skip-search";

export function resolveQueryEffectAction(
  q: string,
  isFollowMode: boolean,
  skipNextSearch: boolean,
): QueryEffectAction {
  if (isFollowMode) return "follow";

  const hasLast = /\blast=\S+/.test(q);
  const hasStart = q.includes("start=");

  if (!hasLast && !hasStart) return "inject-default-range";
  if (skipNextSearch) return "skip-search";
  return "search";
}

export const buildSeverityExpr = (severities: string[]): string => {
  if (severities.length === 0) return "";
  if (severities.length === 1) return `level=${severities[0]}`;
  return `(${severities.map((s) => `level=${s}`).join(" OR ")})`;
};
