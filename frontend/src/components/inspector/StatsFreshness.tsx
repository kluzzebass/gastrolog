import type { Timestamp } from "@bufbuild/protobuf";
import { useThemeClass } from "../../hooks/useThemeClass";
import { protoToInstant, relativeTime } from "../../utils/temporal";

export interface StatsFreshness {
  /** e.g. "sampled 3s ago" */
  label: string;
  /** True once the sample is older than the responding node's stats TTL. */
  stale: boolean;
}

/**
 * Age of a broadcast stats sample, from the responding node's receipt instant.
 * Null when there is no sample to date. `ttlMs` null means the TTL is not
 * known yet, so nothing can be called stale.
 */
export function statsFreshness(
  receivedAt: Timestamp | undefined,
  ttlMs: number | null,
  nowMs: number = Date.now(),
): StatsFreshness | null {
  if (!receivedAt) return null;
  const instant = protoToInstant(receivedAt);
  const ageMs = nowMs - instant.epochMilliseconds;
  return {
    label: `sampled ${relativeTime(instant)}`,
    stale: ttlMs !== null && ageMs > ttlMs,
  };
}

/**
 * The age of a broadcast stats sample — the cue that turns a number on a
 * peer-stats surface from "now" into "as of then". Warns once the sample is
 * older than the responding node's stats TTL: the cluster has stopped
 * serving it, so what is on screen is a copy nothing refreshes.
 */
export function StatsFreshnessCue({
  receivedAt,
  ttlMs,
  dark,
  className,
}: Readonly<{
  receivedAt: Timestamp | undefined;
  ttlMs: number | null;
  dark: boolean;
  className?: string;
}>) {
  const c = useThemeClass(dark);
  const fresh = statsFreshness(receivedAt, ttlMs);
  if (!fresh) return null;
  const tone = fresh.stale
    ? "text-severity-warn"
    : c("text-text-muted", "text-light-text-muted");
  return (
    <span
      className={`text-[0.75em] font-mono whitespace-nowrap ${tone} ${className ?? ""}`}
      title={fresh.stale ? "older than the stats TTL — the cluster no longer serves this sample" : undefined}
    >
      {fresh.label}
      {fresh.stale ? " · stale" : ""}
    </span>
  );
}
