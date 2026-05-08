// Truncate a string in the middle with a unicode ellipsis.
//
// If the string is at most `maxLen` characters, returns it unchanged.
// Otherwise keeps `prefix` characters from the start and `suffix` from
// the end, joined by `…`. When prefix/suffix are omitted, the budget
// is split evenly around the ellipsis.
//
// Use cases in the codebase:
//   - Chunk IDs (32-char base32hex):  middleTruncate(id, 14, 8, 5)
//   - Join tokens (also long):         middleTruncate(token, 13, 8, 4)
//   - Node names in tooltips:          middleTruncate(name, 14)
export function middleTruncate(s: string, maxLen: number, prefix?: number, suffix?: number): string {
  if (s.length <= maxLen) return s;
  if (prefix !== undefined && suffix !== undefined) {
    return s.slice(0, prefix) + "…" + s.slice(-suffix);
  }
  const half = Math.floor((maxLen - 1) / 2);
  return s.slice(0, half) + "…" + s.slice(-half);
}
