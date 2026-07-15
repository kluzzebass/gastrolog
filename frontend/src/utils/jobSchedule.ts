/** Canonical 6-field cron for display (second minute hour dom month dow). */

const EVERY_PREFIX = /^@every\s+(.+)$/i;

function parseGoDuration(s: string): number | null {
  const trimmed = s.trim();
  if (!trimmed) return null;
  let totalMs = 0;
  const re = /(\d+(?:\.\d+)?)(ms|s|m|h)/g;
  let match: RegExpExecArray | null;
  let consumed = "";
  while ((match = re.exec(trimmed)) !== null) {
    const value = Number(match[1]);
    const unit = match[2];
    consumed += match[0];
    if (unit === "ms") totalMs += value;
    else if (unit === "s") totalMs += value * 1000;
    else if (unit === "m") totalMs += value * 60_000;
    else if (unit === "h") totalMs += value * 3_600_000;
  }
  if (consumed !== trimmed) return null;
  return totalMs;
}

function cronEveryMs(ms: number): string {
  if (ms <= 0) return "* * * * * *";
  const sec = Math.max(1, Math.round(ms / 1000));
  if (sec < 60) {
    return sec <= 1 ? "* * * * * *" : `*/${sec} * * * * *`;
  }
  if (ms % 60_000 === 0) {
    const min = ms / 60_000;
    if (min <= 1) return "0 * * * * *";
    if (min < 60) return `0 */${min} * * * *`;
  }
  if (ms % 3_600_000 === 0) {
    const hour = ms / 3_600_000;
    return hour <= 1 ? "0 0 * * * *" : `0 0 */${hour} * * *`;
  }
  return sec <= 1 ? "* * * * * *" : `*/${sec} * * * * *`;
}

export function formatJobSchedule(schedule: string): string {
  const expr = schedule.trim();
  if (!expr || expr === "once") return expr;
  const every = EVERY_PREFIX.exec(expr);
  if (every) {
    const ms = parseGoDuration(every[1] ?? "");
    if (ms != null) return cronEveryMs(ms);
  }
  const fields = expr.split(/\s+/);
  if (fields.length === 5) {
    return `0 ${expr}`;
  }
  return expr;
}
