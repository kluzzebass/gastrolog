// temporal.ts — Temporal API utilities for protobuf timestamp handling.
//
// All protobuf Timestamps flow through protoToInstant() at the boundary,
// preserving nanosecond precision that JavaScript Date discards.

import { Temporal } from "temporal-polyfill";

export type ProtoTimestamp = { seconds: bigint; nanos: number };

/** Convert a protobuf Timestamp to a Temporal.Instant (nanosecond precision). */
export function protoToInstant(ts: ProtoTimestamp): Temporal.Instant {
  const epochNs = ts.seconds * 1_000_000_000n + BigInt(ts.nanos);
  return Temporal.Instant.fromEpochNanoseconds(epochNs);
}

/** Full-precision RFC 3339 string (nanoseconds preserved). */
export function instantToISO(instant: Temporal.Instant): string {
  return instant.toString(); // e.g. "2026-03-12T01:38:25.397123456Z"
}

/** Convert to Date for locale-aware formatting. Loses sub-ms precision. */
export function instantToDate(instant: Temporal.Instant): Date {
  return new Date(instant.epochMilliseconds);
}

/** Epoch milliseconds (for arithmetic and Date-based APIs like HistogramData). */
export function instantToMs(instant: Temporal.Instant): number {
  return instant.epochMilliseconds;
}

// ── Display formatting ──────────────────────────────────────────────

/** Relative time string, e.g. "3s ago", "2m ago". */
export function relativeTime(instant: Temporal.Instant): string {
  const diffMs = Date.now() - instant.epochMilliseconds;
  if (diffMs < 0) return "in the future";
  const sec = Math.floor(diffMs / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const days = Math.floor(hr / 24);
  return `${days}d ago`;
}

/** Format as `YYYY-MM-DD HH:MM:SS` (24-hour, local time). */
export function formatTimestamp(instant: Temporal.Instant): string {
  const d = instantToDate(instant);
  const y = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  const h = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const s = String(d.getSeconds()).padStart(2, "0");
  return `${y}-${mo}-${day} ${h}:${mi}:${s}`;
}

/** Elapsed time since a past instant, e.g. "3m 12s ago", "1h 4m ago". */
export function elapsed(instant: Temporal.Instant, now = Date.now()): string {
  const diff = now - instant.epochMilliseconds;
  if (diff < 0) return "just now";

  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  const remSecs = secs % 60;
  if (mins < 60) return `${mins}m ${remSecs}s ago`;
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  if (hours < 24) return `${hours}h ${remMins}m ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h ago`;
}

/** Countdown to a future instant, e.g. "in 42s", "in 3m 12s". */
export function countdown(instant: Temporal.Instant, now = Date.now()): string {
  const diff = instant.epochMilliseconds - now;
  if (diff <= 0) return "now";

  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `in ${secs}s`;
  const mins = Math.floor(secs / 60);
  const remSecs = secs % 60;
  if (mins < 60) return `in ${mins}m ${remSecs}s`;
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  if (hours < 24) return `in ${hours}h ${remMins}m`;
  const days = Math.floor(hours / 24);
  return `in ${days}d ${hours % 24}h`;
}

/** Locale time with fractional seconds (for log entry timestamps). */
export function formatLocalTime(instant: Temporal.Instant): string {
  return instantToDate(instant).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
    hour12: false,
  });
}

/** Locale time without fractional seconds (for explain/context display). */
export function formatLocalTimeShort(instant: Temporal.Instant): string {
  return instantToDate(instant).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}
