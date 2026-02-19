import { timeRangeMs } from "../utils";

export const stripTimeRange = (q: string): string =>
  q
    .replace(/\blast=\S+/g, "")
    .replace(/\bstart=\S+/g, "")
    .replace(/\bend=\S+/g, "")
    .replace(/\breverse=\S+/g, "")
    .replace(/\s+/g, " ")
    .trim();

export const stripStore = (q: string): string =>
  q
    .replace(/\bstore=\S+/g, "")
    .replace(/\s+/g, " ")
    .trim();

export const stripChunk = (q: string): string =>
  q
    .replace(/\bchunk=\S+/g, "")
    .replace(/\s+/g, " ")
    .trim();

export const stripPos = (q: string): string =>
  q
    .replace(/\bpos=\S+/g, "")
    .replace(/\s+/g, " ")
    .trim();

export const stripSeverity = (qs: string): string =>
  qs
    .replace(/\((?:level=\w+\s+OR\s+)*level=\w+\)/g, "")
    .replace(/\blevel=(?:error|warn|info|debug|trace)\b/g, "")
    .replace(/\bnot\s+level=\*\b/gi, "")
    .replace(/\s+/g, " ")
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
    .replace(/\s+/g, " ")
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

export const buildSeverityExpr = (severities: string[]): string => {
  if (severities.length === 0) return "";
  if (severities.length === 1) return `level=${severities[0]}`;
  return `(${severities.map((s) => `level=${s}`).join(" OR ")})`;
};
