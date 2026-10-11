import type { Timestamp } from "@bufbuild/protobuf";
import { useThemeClass } from "../../hooks/useThemeClass";
import { statsFreshness } from "./sampleFreshness";

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
