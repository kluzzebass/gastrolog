// normalizeTimeDirectives — resolve human-friendly time values in query
// expressions to RFC 3339 before sending to the backend.
//
// Uses the query tokenizer to locate key=value spans for time directives,
// parses values with parseHumanTime, and replaces them in the original
// expression string while preserving all other tokens exactly.

import { lex, type QueryToken } from "../queryTokenizer";
import { parseHumanTime, type ParseOptions } from "./parseHumanTime";

const TIME_DIRECTIVES = new Set([
  "start",
  "end",
  "source_start",
  "source_end",
  "ingest_start",
  "ingest_end",
]);

export function normalizeTimeDirectives(
  expression: string,
  opts?: ParseOptions,
): string {
  const tokens = lex(expression);

  // Collect replacements as [startPos, endPos, replacement] to apply in
  // reverse order so positions stay valid.
  const replacements: [number, number, string][] = [];

  let i = 0;
  while (i < tokens.length) {
    const tok = tokens[i]!;

    // Look for: word = (word|quoted) where word is a time directive.
    if (
      tok.kind === "word" &&
      TIME_DIRECTIVES.has(tok.text.toLowerCase()) &&
      i + 2 < tokens.length &&
      tokens[i + 1]!.kind === "eq"
    ) {
      const valTok = tokens[i + 2]!;
      if (valTok.kind === "word" || valTok.kind === "quoted") {
        const rawValue =
          valTok.kind === "quoted"
            ? valTok.text.slice(1, -1) // strip quotes
            : valTok.text;

        const parsed = parseHumanTime(rawValue, opts);
        if (parsed) {
          const rfc3339 = parsed.toISOString();
          // Replace the value token (and for quoted tokens, the whole token including quotes)
          replacements.push([valTok.pos, valTok.pos + valTok.text.length, rfc3339]);
        }
        i += 3;
        continue;
      }
    }
    i++;
  }

  if (replacements.length === 0) return expression;

  // Apply replacements from end to start to preserve positions.
  let result = expression;
  for (let j = replacements.length - 1; j >= 0; j--) {
    const [start, end, replacement] = replacements[j]!;
    result = result.slice(0, start) + replacement + result.slice(end);
  }
  return result;
}
