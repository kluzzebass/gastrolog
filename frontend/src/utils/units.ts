/** Shared unit formatting and parsing utilities. */

/** Format a number of bytes to a human-readable string (e.g. "1.5 MB"). */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024)
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

/** Format a bigint byte count to a compact string (e.g. "64MB"). */
export function formatBytesBigint(b: bigint): string {
  if (b === 0n) return "";
  if (b >= 1073741824n && b % 1073741824n === 0n) return `${b / 1073741824n}GB`;
  if (b >= 1048576n && b % 1048576n === 0n) return `${b / 1048576n}MB`;
  if (b >= 1024n && b % 1024n === 0n) return `${b / 1024n}KB`;
  return `${b}B`;
}

/** Parse a byte string like "64MB" to bigint. */
export function parseBytes(s: string): bigint {
  s = s.trim().toUpperCase();
  if (!s) return 0n;
  const match = /^(\d+)\s*(GB|MB|KB|B)?$/.exec(s);
  if (!match) return 0n;
  const n = BigInt(match[1]!);
  switch (match[2]) {
    case "GB":
      return n * 1073741824n;
    case "MB":
      return n * 1048576n;
    case "KB":
      return n * 1024n;
    default:
      return n;
  }
}

/** Format seconds (bigint) as human-readable duration (e.g. "1h30m"). */
// eslint-disable-next-line sonarjs/cognitive-complexity -- inherently complex duration formatting with many edge cases
export function formatDuration(s: bigint): string {
  if (s === 0n) return "";
  const days = s / 86400n;
  const hours = (s % 86400n) / 3600n;
  const mins = (s % 3600n) / 60n;
  const secs = s % 60n;
  if (days > 0n && hours === 0n && mins === 0n && secs === 0n)
    return `${days * 24n}h`;
  if (days > 0n && mins === 0n && secs === 0n)
    return `${days * 24n + hours}h`;
  const totalHours = days * 24n + hours;
  if (totalHours > 0n && mins === 0n && secs === 0n) return `${totalHours}h`;
  if (totalHours > 0n && secs === 0n) return `${totalHours}h${mins}m`;
  if (mins > 0n && secs === 0n) return `${mins}m`;
  if (secs > 0n && totalHours === 0n && mins === 0n) return `${secs}s`;
  const hPart = totalHours > 0n ? String(totalHours) + "h" : "";
  const mPart = mins > 0n ? String(mins) + "m" : "";
  const sPart = secs > 0n ? String(secs) + "s" : "";
  return hPart + mPart + sPart;
}

/** Parse a duration string like "720h" or "30d" to seconds as bigint. */
export function parseDuration(s: string): bigint {
  s = s.trim().toLowerCase();
  if (!s) return 0n;
  let total = 0n;
  // eslint-disable-next-line sonarjs/slow-regex -- no backtracking risk: \d+ and [dhms] are disjoint character classes
  const re = /(\d+)([dhms])/g;
  let match;
  while ((match = re.exec(s)) !== null) {
    const n = BigInt(match[1]!);
    switch (match[2]) {
      case "d":
        total += n * 86400n;
        break;
      case "h":
        total += n * 3600n;
        break;
      case "m":
        total += n * 60n;
        break;
      case "s":
        total += n;
        break;
    }
  }
  if (total === 0n && /^\d+$/.test(s)) total = BigInt(s);
  return total;
}

/** Format milliseconds to a human-readable duration (e.g. "2h 15m"). */
export function formatDurationMs(ms: number): string {
  if (ms < 1_000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(0)}s`;
  if (ms < 3600_000) return `${(ms / 60_000).toFixed(0)}m`;
  if (ms < 86400_000) {
    const h = Math.floor(ms / 3600_000);
    const m = Math.floor((ms % 3600_000) / 60_000);
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  }
  const d = Math.floor(ms / 86400_000);
  const h = Math.floor((ms % 86400_000) / 3600_000);
  return h > 0 ? `${d}d ${h}h` : `${d}d`;
}
