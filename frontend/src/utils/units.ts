/** Shared unit formatting and parsing utilities. */

/**
 * Format a byte count to a human-readable string (e.g. "1.5 MiB").
 * Binary math with honest IEC labels: GB means 10^9 and GiB means 2^30 —
 * the parsers on both surfaces are strict about it, so dividing by 1024
 * and printing "MB" would mislabel the quantity. Accepts number or bigint.
 */
export function formatBytes(b: bigint | number): string {
  const n = typeof b === "bigint" ? Number(b) : b;
  if (n === 0) return "0 B";
  if (n >= 1024 ** 4) return `${(n / 1024 ** 4).toFixed(1)} TiB`;
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(1)} GiB`;
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(1)} MiB`;
  if (n >= 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${n} B`;
}

/** Format a per-second rate to a compact count (e.g. "1.5K"); pair with a "/s" suffix. */
export function formatRate(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  if (n >= 10) return Math.round(n).toString();
  return n.toFixed(1);
}

/**
 * Format a bigint byte count to a compact exact string for form echo
 * (e.g. "64MB", "2GiB"). Largest unit that divides evenly wins; at each
 * scale the decimal (SI) unit is preferred so values entered as "2GB"
 * round-trip verbatim, with the binary (IEC) unit as the exact fallback.
 * Values divisible by neither render as raw bytes — exactness over brevity.
 */
export function formatBytesBigint(b: bigint): string {
  if (b === 0n) return "";
  const units: Array<[bigint, string]> = [
    [1_000_000_000_000n, "TB"],
    [1099511627776n, "TiB"],
    [1_000_000_000n, "GB"],
    [1073741824n, "GiB"],
    [1_000_000n, "MB"],
    [1048576n, "MiB"],
    [1_000n, "KB"],
    [1024n, "KiB"],
  ];
  for (const [mult, label] of units) {
    if (b >= mult && b % mult === 0n) return `${b / mult}${label}`;
  }
  return `${b}B`;
}

/**
 * Parse a byte string like "64MB" or "1.5GiB" to bigint. Strict SI/IEC
 * semantics shared with the backend's system.ParseSize: KB/MB/GB/TB are
 * decimal (x1000), KiB/MiB/GiB/TiB are binary (x1024). Returns 0n for
 * empty or unparseable input.
 */
export function parseBytes(s: string): bigint {
  s = s.trim();
  if (!s) return 0n;
  const match = /^(\d+(?:\.\d+)?)\s*(TIB|GIB|MIB|KIB|TB|GB|MB|KB|B)?$/.exec(s.toUpperCase());
  if (!match) return 0n;
  const n = parseFloat(match[1]!);
  const mult: Record<string, number> = {
    B: 1,
    KB: 1_000,
    MB: 1_000_000,
    GB: 1_000_000_000,
    TB: 1_000_000_000_000,
    KIB: 1024,
    MIB: 1024 ** 2,
    GIB: 1024 ** 3,
    TIB: 1024 ** 4,
  };
  return BigInt(Math.round(n * mult[match[2] ?? "B"]!));
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
