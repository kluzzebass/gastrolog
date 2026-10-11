import type { Timestamp } from "@bufbuild/protobuf";
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
