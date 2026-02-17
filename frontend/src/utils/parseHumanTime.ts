// parseHumanTime — parse human-friendly time expressions to Date.
//
// Priority chain:
//   1. RFC3339 passthrough (already valid)
//   2. Unix timestamp passthrough (all digits)
//   3. Relaxed ISO (2024-01-15, 2024-01-15 08:00)
//   4. Locale-aware slash/dot dates (1/15/2024 vs 15/1/2024)
//   5. Time-only (08:00, 14:30)
//   6. Named keywords (now, today, yesterday, tomorrow)
//   7. Period keywords (this morning, this afternoon, this evening, tonight)
//   8. Last day-of-week (last monday ... last sunday)
//   9. Relative phrase (N unit(s) ago)
//
// All local-time outputs → date.toISOString() (UTC).
// Returns null if the value is already RFC3339/Unix (passthrough) or unparseable.

import { subMonths, subYears } from "date-fns";

export interface ParseOptions {
  now?: Date;
  dateOrder?: "mdy" | "dmy" | "ymd";
}

let cachedDateOrder: "mdy" | "dmy" | "ymd" | null = null;

export function detectDateOrder(): "mdy" | "dmy" | "ymd" {
  if (cachedDateOrder) return cachedDateOrder;
  try {
    // Format a date where day, month, year are all distinct and unambiguous.
    // Dec 25, 2033 — month=12, day=25, year=2033
    const d = new Date(2033, 11, 25);
    const fmt = new Intl.DateTimeFormat(navigator.language, {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
    const parts = fmt.formatToParts(d);
    const order: string[] = [];
    for (const p of parts) {
      if (p.type === "month") order.push("m");
      else if (p.type === "day") order.push("d");
      else if (p.type === "year") order.push("y");
    }
    const key = order.join("");
    if (key === "ymd") cachedDateOrder = "ymd";
    else if (key === "dmy") cachedDateOrder = "dmy";
    else cachedDateOrder = "mdy"; // default to US
  } catch {
    cachedDateOrder = "mdy";
  }
  return cachedDateOrder;
}

// Exported for testing — allows resetting cached locale detection.
export function resetDateOrderCache(): void {
  cachedDateOrder = null;
}

export function parseHumanTime(
  value: string,
  opts?: ParseOptions,
): Date | null {
  const v = value.trim();
  if (!v) return null;

  const now = opts?.now ?? new Date();

  // 1. RFC3339 passthrough — already valid, no transform needed.
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(v)) return null;

  // 2. Unix timestamp passthrough — all digits.
  if (/^\d+$/.test(v)) return null;

  // 3. Relaxed ISO: 2024-01-15, 2024-01-15 08:00, 2024-01-15T08:00
  const relaxed = parseRelaxedISO(v);
  if (relaxed) return relaxed;

  // 4. Locale-aware slash/dot dates: 1/15/2024, 15.1.2024
  const slashDot = parseSlashDotDate(v, opts?.dateOrder ?? detectDateOrder());
  if (slashDot) return slashDot;

  // 5. Time-only: 08:00, 14:30, 8:00
  const timeOnly = parseTimeOnly(v, now);
  if (timeOnly) return timeOnly;

  const lower = v.toLowerCase();

  // 6. Named keywords
  const keyword = parseNamedKeyword(lower, now);
  if (keyword) return keyword;

  // 7. Period keywords
  const period = parsePeriodKeyword(lower, now);
  if (period) return period;

  // 8. Last day-of-week
  const lastDay = parseLastDayOfWeek(lower, now);
  if (lastDay) return lastDay;

  // 9. Relative phrase: N unit(s) ago
  const relative = parseRelativePhrase(lower, now);
  if (relative) return relative;

  return null;
}

// --- Sub-parsers ---

function parseRelaxedISO(v: string): Date | null {
  // Match: 2024-01-15, 2024-01-15 08:00, 2024-01-15T08:00, 2024-01-15 08:00:30
  // Must NOT have timezone suffix (that would be RFC3339).
  const m = v.match(
    /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/,
  );
  if (!m) return null;
  const [, ys, ms, ds, hs, mins, ss] = m;
  const year = parseInt(ys!, 10);
  const month = parseInt(ms!, 10);
  const day = parseInt(ds!, 10);
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const hour = hs ? parseInt(hs, 10) : 0;
  const minute = mins ? parseInt(mins, 10) : 0;
  const second = ss ? parseInt(ss, 10) : 0;
  if (hour > 23 || minute > 59 || second > 59) return null;
  return new Date(year, month - 1, day, hour, minute, second);
}

function parseSlashDotDate(
  v: string,
  dateOrder: "mdy" | "dmy" | "ymd",
): Date | null {
  // Match: 1/15/2024, 15.1.2024, 2024/01/15, etc.
  const m = v.match(/^(\d{1,4})[/.](\d{1,2})[/.](\d{1,4})$/);
  if (!m) return null;
  const [, a, b, c] = m;
  const n1 = parseInt(a!, 10);
  const n2 = parseInt(b!, 10);
  const n3 = parseInt(c!, 10);

  let year: number, month: number, day: number;

  // 4-digit first component → YMD regardless of locale
  if (a!.length === 4) {
    year = n1;
    month = n2;
    day = n3;
  } else if (c!.length === 4) {
    // 4-digit last component → use locale order for first two
    year = n3;
    if (dateOrder === "dmy") {
      day = n1;
      month = n2;
    } else {
      // mdy (default)
      month = n1;
      day = n2;
    }
  } else {
    // All short numbers — ambiguous, try locale order with 2-digit year
    return null;
  }

  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  return new Date(year, month - 1, day);
}

function parseTimeOnly(v: string, now: Date): Date | null {
  const m = v.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;
  const hour = parseInt(m[1]!, 10);
  const minute = parseInt(m[2]!, 10);
  const second = m[3] ? parseInt(m[3], 10) : 0;
  if (hour > 23 || minute > 59 || second > 59) return null;
  const d = new Date(now);
  d.setHours(hour, minute, second, 0);
  return d;
}

const NAMED_KEYWORDS: Record<string, (now: Date) => Date> = {
  now: (now) => new Date(now),
  today: (now) => startOfDay(now),
  yesterday: (now) => {
    const d = startOfDay(now);
    d.setDate(d.getDate() - 1);
    return d;
  },
  tomorrow: (now) => {
    const d = startOfDay(now);
    d.setDate(d.getDate() + 1);
    return d;
  },
};

function parseNamedKeyword(lower: string, now: Date): Date | null {
  const fn = NAMED_KEYWORDS[lower];
  return fn ? fn(now) : null;
}

const PERIOD_KEYWORDS: Record<string, (now: Date) => Date> = {
  "this morning": (now) => {
    const d = startOfDay(now);
    d.setHours(6);
    return d;
  },
  "this afternoon": (now) => {
    const d = startOfDay(now);
    d.setHours(12);
    return d;
  },
  "this evening": (now) => {
    const d = startOfDay(now);
    d.setHours(18);
    return d;
  },
  tonight: (now) => {
    const d = startOfDay(now);
    d.setHours(18);
    return d;
  },
};

function parsePeriodKeyword(lower: string, now: Date): Date | null {
  const fn = PERIOD_KEYWORDS[lower];
  return fn ? fn(now) : null;
}

const DAY_NAMES: Record<string, number> = {
  sunday: 0,
  monday: 1,
  tuesday: 2,
  wednesday: 3,
  thursday: 4,
  friday: 5,
  saturday: 6,
};

function parseLastDayOfWeek(lower: string, now: Date): Date | null {
  const m = lower.match(/^last\s+(\w+)$/);
  if (!m) return null;
  const target = DAY_NAMES[m[1]!];
  if (target === undefined) return null;
  const current = now.getDay();
  // How many days back to the most recent occurrence of target.
  // If today is the same weekday, "last monday" means 7 days ago.
  let diff = current - target;
  if (diff <= 0) diff += 7;
  const d = startOfDay(now);
  d.setDate(d.getDate() - diff);
  return d;
}

const UNIT_MAP: Record<string, string> = {
  s: "second",
  sec: "second",
  secs: "second",
  second: "second",
  seconds: "second",
  m: "minute",
  min: "minute",
  mins: "minute",
  minute: "minute",
  minutes: "minute",
  h: "hour",
  hr: "hour",
  hrs: "hour",
  hour: "hour",
  hours: "hour",
  d: "day",
  day: "day",
  days: "day",
  w: "week",
  wk: "week",
  wks: "week",
  week: "week",
  weeks: "week",
  mo: "month",
  month: "month",
  months: "month",
  y: "year",
  yr: "year",
  yrs: "year",
  year: "year",
  years: "year",
};

function parseRelativePhrase(lower: string, now: Date): Date | null {
  const m = lower.match(/^(\d+)\s*(\w+)\s+ago$/);
  if (!m) return null;
  const n = parseInt(m[1]!, 10);
  const unit = UNIT_MAP[m[2]!];
  if (!unit) return null;

  const d = new Date(now);
  switch (unit) {
    case "second":
      d.setSeconds(d.getSeconds() - n);
      break;
    case "minute":
      d.setMinutes(d.getMinutes() - n);
      break;
    case "hour":
      d.setHours(d.getHours() - n);
      break;
    case "day":
      d.setDate(d.getDate() - n);
      break;
    case "week":
      d.setDate(d.getDate() - n * 7);
      break;
    case "month":
      return subMonths(d, n);
    case "year":
      return subYears(d, n);
  }
  return d;
}

function startOfDay(d: Date): Date {
  const r = new Date(d);
  r.setHours(0, 0, 0, 0);
  return r;
}
