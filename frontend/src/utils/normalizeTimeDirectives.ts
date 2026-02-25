// normalizeTimeDirectives — resolve human-friendly time values in query
// expressions to RFC 3339 before sending to the backend.
//
// Uses a simple regex to locate key=value spans for time directives,
// parses values with parseHumanTime, and replaces them in the original
// expression string.

import { parseHumanTime, type ParseOptions } from "./parseHumanTime";

const TIME_DIR_RE =
  /\b(start|end|source_start|source_end|ingest_start|ingest_end)=(?:"([^"]*?)"|'([^']*?)'|(\S+))/g;

export function normalizeTimeDirectives(
  expression: string,
  opts?: ParseOptions,
): string {
  return expression.replace(TIME_DIR_RE, (match, key: string, dq?: string, sq?: string, bare?: string) => {
    const rawValue = dq ?? sq ?? bare;
    if (!rawValue) return match;
    const parsed = parseHumanTime(rawValue, opts);
    return parsed ? `${key}=${parsed.toISOString()}` : match;
  });
}
