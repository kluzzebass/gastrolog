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

/**
 * Count suffixes, largest first. K/M/B/T are the short-scale names; past
 * trillion the ladder continues with the SI letters P and E, which agree
 * with T (tera = trillion) and with the PB/EB byte units shown alongside.
 * E covers the whole uint64 range (max ≈ 18.4E).
 */
const COUNT_SCALES: ReadonlyArray<readonly [bigint, string]> = [
  [1_000_000_000_000_000_000n, "E"],
  [1_000_000_000_000_000n, "P"],
  [1_000_000_000_000n, "T"],
  [1_000_000_000n, "B"],
  [1_000_000n, "M"],
  [1_000n, "K"],
];

function wholeCount(n: bigint | number | string): bigint | undefined {
  if (typeof n === "bigint") return n;
  if (typeof n === "string") {
    const s = n.trim();
    return /^\d+$/.test(s) ? BigInt(s) : wholeCount(Number(s));
  }
  return Number.isFinite(n) ? BigInt(Math.trunc(n)) : undefined;
}

/** One decimal of the largest scale the value reaches, rounded half-up in exact integer math. */
function formatScaledCount(v: bigint): string {
  let i = COUNT_SCALES.findIndex(([div]) => v >= div);
  let [div, suffix] = COUNT_SCALES[i]!;
  let tenths = (v * 10n + div / 2n) / div;
  if (tenths >= 10_000n && i > 0) {
    i--;
    [div, suffix] = COUNT_SCALES[i]!;
    tenths = (v * 10n + div / 2n) / div;
  }
  return `${tenths / 10n}.${tenths % 10n}${suffix}`;
}

/**
 * Format a count compactly (e.g. "1.5K", "25.6B", "18.4E"). Exact across the
 * full uint64 range: from a thousand up the scale and digits come from bigint
 * math, so values past 2^53 lose nothing and a value that rounds to 1000.0 of
 * one scale renders as 1.0 of the next. Below a thousand it is locale digits.
 */
export function formatCount(n: bigint | number | string): string {
  const whole = wholeCount(n);
  if (whole === undefined || whole < 1_000n) return Number(n).toLocaleString();
  return formatScaledCount(whole);
}

/**
 * Format a count rounded to 2 significant figures (e.g. "190K", "2.5B") so
 * the imprecision of an estimate is visible in the digits themselves —
 * "188,093" reads like a ground-truth count, "190K" like an approximation.
 */
export function formatApproxCount(n: number): string {
  if (n < 100) return n.toString();
  const magnitude = 10 ** (Math.floor(Math.log10(n)) - 1);
  const rounded = Math.round(n / magnitude) * magnitude;
  if (rounded < 1000) return rounded.toString();
  const [div, suffix] = COUNT_SCALES.find(([d]) => rounded >= Number(d))!;
  const v = rounded / Number(div);
  return v < 10 ? `${v.toFixed(1)}${suffix}` : `${Math.round(v)}${suffix}`;
}

/** Format a per-second rate to a compact count (e.g. "1.5K"); pair with a "/s" suffix. */
export function formatRate(n: number): string {
  if (Math.round(n) >= 1_000) return formatCount(Math.max(n, 1_000));
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
    [1_000_000_000_000_000_000n, "EB"],
    [1152921504606846976n, "EiB"],
    [1_000_000_000_000_000n, "PB"],
    [1125899906842624n, "PiB"],
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
  s = s.replace(/\s+/g, "");
  if (!s) return 0n;
  const match = /^(\d+(?:\.\d+)?)\s*(EIB|PIB|TIB|GIB|MIB|KIB|EB|PB|TB|GB|MB|KB|B)?$/.exec(s.toUpperCase());
  if (!match) return 0n;
  // BigInt multipliers so large units (PiB, EiB) stay exact — Number math
  // loses precision above 2^53, which would corrupt an EiB value on the way
  // to the wire. Keep parity with the backend's ParseSize.
  const mult: Record<string, bigint> = {
    B: 1n,
    KB: 1_000n,
    MB: 1_000_000n,
    GB: 1_000_000_000n,
    TB: 1_000_000_000_000n,
    PB: 1_000_000_000_000_000n,
    EB: 1_000_000_000_000_000_000n,
    KIB: 1024n,
    MIB: 1024n ** 2n,
    GIB: 1024n ** 3n,
    TIB: 1024n ** 4n,
    PIB: 1024n ** 5n,
    EIB: 1024n ** 6n,
  };
  const unit = mult[match[2] ?? "B"]!;
  const num = match[1]!;
  // Integer numeric part → exact BigInt math (the common config case).
  // Fractional part → Number math, acceptable only for the smaller units
  // where fractions are actually used ("1.5TB"); nobody writes "1.5EiB".
  if (!num.includes(".")) {
    return BigInt(num) * unit;
  }
  return BigInt(Math.round(parseFloat(num) * Number(unit)));
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

/**
 * Parse a duration string to nanoseconds at full precision. Accepts Go
 * duration syntax (h/m/s/ms/us/ns, decimals allowed, e.g. "2h3m10s1004ms")
 * plus "d" (days) as input convenience and a bare integer meaning seconds —
 * matching the CLI edge. Returns 0n for empty or unparseable input.
 */
export function parseDurationNanos(s: string): bigint {
  s = s.trim().toLowerCase();
  if (!s) return 0n;
  if (/^\d+$/.test(s)) return BigInt(s) * 1_000_000_000n;
  let total = 0n;
  let rest = s;
  const mult: Record<string, number> = {
    d: 86_400e9, h: 3_600e9, m: 60e9, s: 1e9, ms: 1e6, us: 1e3, ns: 1,
  };
  while (rest.length > 0) {
    const m = /^(\d+(?:\.\d+)?)(d|h|ms|m|s|us|ns)/.exec(rest);
    if (!m) return 0n;
    total += BigInt(Math.round(parseFloat(m[1]!) * mult[m[2]!]!));
    rest = rest.slice(m[0].length);
  }
  return total;
}

/**
 * Format nanoseconds as a canonical exact duration (e.g. "2h3m11.004s",
 * "720h", "30s"). Value-faithful, not spelling-faithful: the operator's
 * exact input form is not preserved, its value is — sub-second precision
 * renders as fractional seconds with trailing zeros trimmed.
 */
export function formatDurationNanos(nanos: bigint): string {
  if (nanos <= 0n) return "";
  const totalSecs = nanos / 1_000_000_000n;
  const frac = nanos % 1_000_000_000n;
  if (frac === 0n) return formatDuration(totalSecs);
  const hours = totalSecs / 3600n;
  const mins = (totalSecs % 3600n) / 60n;
  const secs = totalSecs % 60n;
  const fracStr = frac.toString().padStart(9, "0").replace(/0+$/, "");
  const hPart = hours > 0n ? `${hours}h` : "";
  const mPart = mins > 0n ? `${mins}m` : "";
  return `${hPart}${mPart}${secs}.${fracStr}s`;
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
