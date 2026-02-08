// Cron expression validator and describer.
// Supports both standard 5-field (minute-level) and 6-field (second-level) syntax.
//
// 5-field: minute hour day-of-month month day-of-week
// 6-field: second minute hour day-of-month month day-of-week
//
// Ranges: seconds 0-59, minutes 0-59, hours 0-23, day 1-31, month 1-12, dow 0-6 (0=Sunday)
// Note: gocron uses 0-6 for day-of-week (not 0-7).

const FIELD_NAMES_5 = [
  "minute",
  "hour",
  "day-of-month",
  "month",
  "day-of-week",
];
const FIELD_NAMES_6 = [
  "second",
  "minute",
  "hour",
  "day-of-month",
  "month",
  "day-of-week",
];

const FIELD_RANGES_5: [number, number][] = [
  [0, 59],
  [0, 23],
  [1, 31],
  [1, 12],
  [0, 6],
];

const FIELD_RANGES_6: [number, number][] = [
  [0, 59],
  [0, 59],
  [0, 23],
  [1, 31],
  [1, 12],
  [0, 6],
];

const MONTH_NAMES: Record<string, number> = {
  jan: 1,
  feb: 2,
  mar: 3,
  apr: 4,
  may: 5,
  jun: 6,
  jul: 7,
  aug: 8,
  sep: 9,
  oct: 10,
  nov: 11,
  dec: 12,
};

const DOW_NAMES: Record<string, number> = {
  sun: 0,
  mon: 1,
  tue: 2,
  wed: 3,
  thu: 4,
  fri: 5,
  sat: 6,
};

function validateField(
  field: string,
  min: number,
  max: number,
  name: string,
  names?: Record<string, number>,
): string | null {
  if (field === "*") return null;

  // Split on commas for list values.
  const parts = field.split(",");
  for (const part of parts) {
    // Step value: */5 or 1-30/5
    const stepMatch = part.match(/^(.+)\/(\d+)$/);
    const base = stepMatch ? stepMatch[1]! : part;
    const step = stepMatch ? parseInt(stepMatch[2]!, 10) : null;

    if (step !== null && (step < 1 || isNaN(step))) {
      return `invalid step value in ${name}`;
    }

    if (base === "*") {
      // */N is valid.
      continue;
    }

    // Range: 1-5
    const rangeMatch = base.match(/^([a-zA-Z0-9]+)-([a-zA-Z0-9]+)$/);
    if (rangeMatch) {
      const lo = parseFieldValue(rangeMatch[1]!, min, max, names);
      const hi = parseFieldValue(rangeMatch[2]!, min, max, names);
      if (lo === null)
        return `invalid range start in ${name}: ${rangeMatch[1]}`;
      if (hi === null) return `invalid range end in ${name}: ${rangeMatch[2]}`;
      if (lo > hi) return `invalid range in ${name}: start > end`;
      continue;
    }

    // Single value.
    const val = parseFieldValue(base, min, max, names);
    if (val === null) return `invalid value in ${name}: ${base}`;
  }

  return null;
}

function parseFieldValue(
  s: string,
  min: number,
  max: number,
  names?: Record<string, number>,
): number | null {
  const lower = s.toLowerCase();
  if (names && lower in names) {
    return names[lower]!;
  }
  const n = parseInt(s, 10);
  if (isNaN(n) || n < min || n > max) return null;
  return n;
}

export function validateCron(expr: string): { valid: boolean; error?: string } {
  const trimmed = expr.trim();
  if (!trimmed) return { valid: false, error: "cron expression is required" };

  const fields = trimmed.split(/\s+/);
  if (fields.length !== 5 && fields.length !== 6) {
    return {
      valid: false,
      error: `expected 5 or 6 fields (second? minute hour day month weekday), got ${fields.length}`,
    };
  }

  const fieldNames = fields.length === 6 ? FIELD_NAMES_6 : FIELD_NAMES_5;
  const fieldRanges = fields.length === 6 ? FIELD_RANGES_6 : FIELD_RANGES_5;

  for (let i = 0; i < fields.length; i++) {
    const [min, max] = fieldRanges[i]!;
    // Month names for month field, DOW names for dow field.
    const monthIdx = fields.length === 6 ? 4 : 3;
    const dowIdx = fields.length === 6 ? 5 : 4;
    const names =
      i === monthIdx ? MONTH_NAMES : i === dowIdx ? DOW_NAMES : undefined;
    const err = validateField(fields[i]!, min, max, fieldNames[i]!, names);
    if (err) return { valid: false, error: err };
  }

  return { valid: true };
}

// Parsed fields in a normalized order for description generation.
interface CronFields {
  second: string | null; // null for 5-field expressions
  minute: string;
  hour: string;
  dom: string;
  month: string;
  dow: string;
}

function parseCronFields(expr: string): CronFields | null {
  const fields = expr.trim().split(/\s+/);
  if (fields.length === 6) {
    return {
      second: fields[0]!,
      minute: fields[1]!,
      hour: fields[2]!,
      dom: fields[3]!,
      month: fields[4]!,
      dow: fields[5]!,
    };
  }
  if (fields.length === 5) {
    return {
      second: null,
      minute: fields[0]!,
      hour: fields[1]!,
      dom: fields[2]!,
      month: fields[3]!,
      dow: fields[4]!,
    };
  }
  return null;
}

export function describeCron(expr: string): string {
  const f = parseCronFields(expr);
  if (!f) return "";

  const { second, minute, hour, dom, month, dow } = f;
  const allStar = (s: string) => s === "*";
  const hasSeconds = second !== null;
  const secondStar = second === null || allStar(second);

  // Every second.
  if (
    hasSeconds &&
    allStar(second!) &&
    allStar(minute) &&
    allStar(hour) &&
    allStar(dom) &&
    allStar(month) &&
    allStar(dow)
  ) {
    return "Every second";
  }

  // Every N seconds.
  if (
    hasSeconds &&
    second!.startsWith("*/") &&
    allStar(minute) &&
    allStar(hour) &&
    allStar(dom) &&
    allStar(month) &&
    allStar(dow)
  ) {
    const n = second!.slice(2);
    return `Every ${n} second${n === "1" ? "" : "s"}`;
  }

  // Every minute (5-field: all stars; 6-field: second=0, rest stars).
  if (
    (secondStar || second === "0") &&
    allStar(minute) &&
    allStar(hour) &&
    allStar(dom) &&
    allStar(month) &&
    allStar(dow)
  ) {
    if (hasSeconds && allStar(second!)) {
      // already handled above as "every second"
    }
    return "Every minute";
  }

  // Every N minutes.
  if (
    (secondStar || second === "0") &&
    minute.startsWith("*/") &&
    allStar(hour) &&
    allStar(dom) &&
    allStar(month) &&
    allStar(dow)
  ) {
    const n = minute.slice(2);
    return `Every ${n} minute${n === "1" ? "" : "s"}`;
  }

  // Every hour at minute N.
  if (
    (secondStar || second === "0") &&
    /^\d+$/.test(minute) &&
    allStar(hour) &&
    allStar(dom) &&
    allStar(month) &&
    allStar(dow)
  ) {
    return minute === "0"
      ? "Every hour, on the hour"
      : `Every hour at minute ${minute}`;
  }

  // Every N hours.
  if (
    (secondStar || second === "0") &&
    minute === "0" &&
    hour.startsWith("*/") &&
    allStar(dom) &&
    allStar(month) &&
    allStar(dow)
  ) {
    const n = hour.slice(2);
    return `Every ${n} hour${n === "1" ? "" : "s"}`;
  }

  // Daily at specific time.
  if (
    (secondStar || /^\d+$/.test(second!)) &&
    /^\d+$/.test(minute) &&
    /^\d+$/.test(hour) &&
    allStar(dom) &&
    allStar(month) &&
    allStar(dow)
  ) {
    const h = parseInt(hour, 10);
    const m = parseInt(minute, 10);
    const s = second !== null ? parseInt(second, 10) : 0;
    const time = formatTime(h, m, hasSeconds ? s : null);
    if (h === 0 && m === 0 && s === 0) return `Daily at midnight`;
    if (h === 12 && m === 0 && s === 0) return `Daily at noon`;
    return `Daily at ${time}`;
  }

  // Weekly.
  if (
    (secondStar || /^\d+$/.test(second!)) &&
    /^\d+$/.test(minute) &&
    /^\d+$/.test(hour) &&
    allStar(dom) &&
    allStar(month) &&
    /^\d+$/.test(dow)
  ) {
    const days = [
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
    ];
    const d = parseInt(dow, 10);
    const dayName = days[d] ?? `day ${d}`;
    const h = parseInt(hour, 10);
    const m = parseInt(minute, 10);
    const s = second !== null ? parseInt(second, 10) : 0;
    const time = formatTime(h, m, hasSeconds ? s : null);
    return `Every ${dayName} at ${time}`;
  }

  // Monthly.
  if (
    (secondStar || /^\d+$/.test(second!)) &&
    /^\d+$/.test(minute) &&
    /^\d+$/.test(hour) &&
    /^\d+$/.test(dom) &&
    allStar(month) &&
    allStar(dow)
  ) {
    const h = parseInt(hour, 10);
    const m = parseInt(minute, 10);
    const s = second !== null ? parseInt(second, 10) : 0;
    const time = formatTime(h, m, hasSeconds ? s : null);
    const d = parseInt(dom, 10);
    const suffix = d === 1 ? "st" : d === 2 ? "nd" : d === 3 ? "rd" : "th";
    return `Monthly on the ${d}${suffix} at ${time}`;
  }

  // Fallback: field-by-field summary.
  const parts: string[] = [];
  if (hasSeconds && !secondStar) parts.push(`second: ${second}`);
  if (!allStar(minute)) parts.push(`minute: ${minute}`);
  if (!allStar(hour)) parts.push(`hour: ${hour}`);
  if (!allStar(dom)) parts.push(`day: ${dom}`);
  if (!allStar(month)) parts.push(`month: ${month}`);
  if (!allStar(dow)) parts.push(`weekday: ${dow}`);
  return parts.join(", ");
}

function formatTime(h: number, m: number, s: number | null): string {
  const hh = h.toString().padStart(2, "0");
  const mm = m.toString().padStart(2, "0");
  if (s !== null && s !== 0) {
    const ss = s.toString().padStart(2, "0");
    return `${hh}:${mm}:${ss}`;
  }
  return `${hh}:${mm}`;
}
