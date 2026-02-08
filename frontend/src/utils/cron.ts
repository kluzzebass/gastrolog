// Standard 5-field cron: minute hour day-of-month month day-of-week
// Ranges: 0-59, 0-23, 1-31, 1-12, 0-7 (0 and 7 = Sunday)

const FIELD_NAMES = ["minute", "hour", "day-of-month", "month", "day-of-week"];
const FIELD_RANGES: [number, number][] = [
  [0, 59],
  [0, 23],
  [1, 31],
  [1, 12],
  [0, 7],
];

const MONTH_NAMES: Record<string, number> = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
};

const DOW_NAMES: Record<string, number> = {
  sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6,
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
      if (lo === null) return `invalid range start in ${name}: ${rangeMatch[1]}`;
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
  if (fields.length !== 5) {
    return {
      valid: false,
      error: `expected 5 fields (minute hour day month weekday), got ${fields.length}`,
    };
  }

  for (let i = 0; i < 5; i++) {
    const [min, max] = FIELD_RANGES[i]!;
    const names = i === 3 ? MONTH_NAMES : i === 4 ? DOW_NAMES : undefined;
    const err = validateField(fields[i]!, min, max, FIELD_NAMES[i]!, names);
    if (err) return { valid: false, error: err };
  }

  return { valid: true };
}

export function describeCron(expr: string): string {
  const fields = expr.trim().split(/\s+/);
  if (fields.length !== 5) return "";

  const [minute, hour, dom, month, dow] = fields as [
    string, string, string, string, string,
  ];
  const allStar = (f: string) => f === "*";

  // Every minute.
  if (allStar(minute) && allStar(hour) && allStar(dom) && allStar(month) && allStar(dow)) {
    return "Every minute";
  }

  // Every N minutes.
  if (minute.startsWith("*/") && allStar(hour) && allStar(dom) && allStar(month) && allStar(dow)) {
    const n = minute.slice(2);
    return `Every ${n} minute${n === "1" ? "" : "s"}`;
  }

  // Every hour at minute N.
  if (/^\d+$/.test(minute) && allStar(hour) && allStar(dom) && allStar(month) && allStar(dow)) {
    return minute === "0" ? "Every hour, on the hour" : `Every hour at minute ${minute}`;
  }

  // Every N hours.
  if (minute === "0" && hour.startsWith("*/") && allStar(dom) && allStar(month) && allStar(dow)) {
    const n = hour.slice(2);
    return `Every ${n} hour${n === "1" ? "" : "s"}`;
  }

  // Daily at specific time.
  if (/^\d+$/.test(minute) && /^\d+$/.test(hour) && allStar(dom) && allStar(month) && allStar(dow)) {
    const h = parseInt(hour, 10);
    const m = parseInt(minute, 10);
    if (h === 0 && m === 0) return "Daily at midnight";
    if (h === 12 && m === 0) return "Daily at noon";
    const time = `${h.toString().padStart(2, "0")}:${m.toString().padStart(2, "0")}`;
    return `Daily at ${time}`;
  }

  // Weekly.
  if (/^\d+$/.test(minute) && /^\d+$/.test(hour) && allStar(dom) && allStar(month) && /^\d+$/.test(dow)) {
    const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    const d = parseInt(dow, 10);
    const dayName = days[d === 7 ? 0 : d] ?? `day ${d}`;
    const h = parseInt(hour, 10);
    const m = parseInt(minute, 10);
    const time = `${h.toString().padStart(2, "0")}:${m.toString().padStart(2, "0")}`;
    return `Every ${dayName} at ${time}`;
  }

  // Monthly.
  if (/^\d+$/.test(minute) && /^\d+$/.test(hour) && /^\d+$/.test(dom) && allStar(month) && allStar(dow)) {
    const h = parseInt(hour, 10);
    const m = parseInt(minute, 10);
    const time = `${h.toString().padStart(2, "0")}:${m.toString().padStart(2, "0")}`;
    const d = parseInt(dom, 10);
    const suffix = d === 1 ? "st" : d === 2 ? "nd" : d === 3 ? "rd" : "th";
    return `Monthly on the ${d}${suffix} at ${time}`;
  }

  // Fallback: field-by-field summary.
  const parts: string[] = [];
  if (!allStar(minute)) parts.push(`minute: ${minute}`);
  if (!allStar(hour)) parts.push(`hour: ${hour}`);
  if (!allStar(dom)) parts.push(`day: ${dom}`);
  if (!allStar(month)) parts.push(`month: ${month}`);
  if (!allStar(dow)) parts.push(`weekday: ${dow}`);
  return parts.join(", ");
}
