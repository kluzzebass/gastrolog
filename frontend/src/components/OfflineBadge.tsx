import type { Timestamp } from "../api/model/node";
import { Badge } from "./Badge";
import { formatDuration } from "../utils/units";
import { useTick } from "./inspector/JobCard";

// How long a node has been silent is a cluster fact, so it comes from the
// cluster: ClusterNode.last_seen, which the backend fills from
// PeerState.LastSeen (the max of last Raft contact and last stats broadcast).
//
// Timing the duration in the browser instead — from the moment THIS TAB first
// saw stats go missing — measured how long the tab had been open, not how long
// the node had been gone: two tabs disagreed, a reload reset it, and a node
// down for hours read "offline 5s" to whoever had just opened the panel.
//
// The local clock still appears here, in useTick and in the elapsed
// subtraction, and that is fine — the ORIGIN is authoritative and only the
// "how long ago was it" arithmetic is local, exactly as NodeStateBadge does it
// with state_since. What must never come from the browser is the instant being
// measured from.

interface OfflineBadgeProps {
  /** Cluster's last positive evidence of life; undefined if never seen. */
  lastSeen?: Timestamp;
  isOffline: boolean;
  dark: boolean;
}

export function OfflineBadge({ lastSeen, isOffline, dark }: Readonly<OfflineBadgeProps>) {
  const now = useTick();
  if (!isOffline) return null;

  // No last_seen means the cluster has never had positive evidence for this
  // node — a deliberate signal from PeerState.LastSeen, not a missing value to
  // paper over. Say "offline" and claim nothing about duration.
  let text = "offline";
  const seenSecs = lastSeen?.seconds ?? 0n;
  if (seenSecs > 0n) {
    const elapsed = BigInt(Math.floor(now / 1000)) - seenSecs;
    if (elapsed >= 1n) {
      text = `offline ${formatDuration(elapsed)}`;
    }
  }

  return (
    <Badge variant="error" dark={dark} title="cluster has not heard from this node">
      {text}
    </Badge>
  );
}
