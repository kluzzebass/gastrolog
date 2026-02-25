import type { PipelineStep } from "../api/client";

// ── Highlight helpers for ExplainPanel ──

export type Range = [number, number]; // [startIdx, endIdx) in expression string

export function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Find all non-overlapping matches of a regex in the expression, return char ranges. */
export function findRanges(expression: string, re: RegExp): Range[] {
  const ranges: Range[] = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(expression)) !== null) {
    ranges.push([m.index, m.index + m[0].length]);
  }
  return ranges;
}

/** Find a bare word in the expression that isn't part of a key=value token. */
export function findBareWordRanges(expression: string, word: string): Range[] {
  const re = new RegExp(`(?<!=)\\b${escapeRegex(word)}\\b(?!=)`, "gi");
  return findRanges(expression, re);
}

/** Map a pipeline step to character ranges in the expression string. */
export function stepToRanges(step: PipelineStep, expression: string): Range[] {
  switch (step.name) {
    case "time":
      return [
        ...findRanges(expression, /\bstart=\S+/g),
        ...findRanges(expression, /\bend=\S+/g),
      ];

    case "token": {
      const inner = /^token\((.+)\)$/.exec(step.predicate)?.[1];
      if (!inner) return [];
      return inner
        .split(/,\s*/)
        .flatMap((w) => findBareWordRanges(expression, w));
    }

    case "kv": {
      // Predicate is the literal token, e.g. "level=error"
      const re = new RegExp(`\\b${escapeRegex(step.predicate)}\\b`, "gi");
      return findRanges(expression, re);
    }

    default:
      return [];
  }
}

/** Split expression into segments: [{text, highlighted}] based on ranges. */
export function buildSegments(
  expression: string,
  ranges: Range[],
): { text: string; highlighted: boolean }[] {
  if (ranges.length === 0) return [{ text: expression, highlighted: false }];

  // Sort and merge overlapping ranges.
  const sorted = [...ranges].sort((a, b) => a[0] - b[0]);
  const merged: Range[] = [sorted[0]!];
  for (let i = 1; i < sorted.length; i++) {
    const prev = merged.at(-1)!;
    if (sorted[i]![0] <= prev[1]) {
      prev[1] = Math.max(prev[1], sorted[i]![1]);
    } else {
      merged.push(sorted[i]!);
    }
  }

  const segments: { text: string; highlighted: boolean }[] = [];
  let cursor = 0;
  for (const [start, end] of merged) {
    if (cursor < start) {
      segments.push({
        text: expression.slice(cursor, start),
        highlighted: false,
      });
    }
    segments.push({ text: expression.slice(start, end), highlighted: true });
    cursor = end;
  }
  if (cursor < expression.length) {
    segments.push({ text: expression.slice(cursor), highlighted: false });
  }
  return segments;
}

/** Format a proto Timestamp for display. */
export function formatTs(ts?: { toDate(): Date }): string {
  if (!ts) return "";
  return ts.toDate().toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

/** Return Tailwind classes for a pipeline step action badge. */
export function actionColor(action: string): string {
  switch (action) {
    case "seek":
    case "indexed":
      return "bg-severity-info/20 text-severity-info border-severity-info/30";
    case "skipped":
      return "bg-severity-error/15 text-severity-error border-severity-error/25";
    default:
      return "bg-severity-warn/15 text-severity-warn border-severity-warn/25";
  }
}
