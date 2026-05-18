import { Badge } from "./Badge";
import { formatDuration } from "../utils/units";
import { useTick } from "./inspector/JobCard";

// gastrolog-778iv: surface time-since-last-heartbeat for peer nodes
// whose stats have gone silent. The 0–5 min window between "peer
// stopped broadcasting" and "unreachable_sweep flips state to
// Unreachable" was previously a blank "offline" badge with no
// duration; now it shows "offline Xs" / "offline Xm".
//
// Shape per issue: client-side tracking from local viewing-node
// observation, no proto change. Module-level Map persists during the
// SPA session. Page reload loses the offline-since value for nodes
// already offline, falling back to "offline" without duration — the
// issue documents this as the trade-off of shape 1.
//
// Seamless handoff to NodeStateBadge: the unreachable_sweep now
// records StateSince = lastSeen (not the wall-clock moment the sweep
// noticed the lapse), so NodeStateBadge's "unreachable Xm" picks up
// the same elapsed value this badge was showing as "offline Xs". The
// visible jump is the label change (offline → unreachable), not the
// counter resetting to zero.
const offlineSince = new Map<string, number>();

// recordOffline returns the timestamp (ms epoch) when nodeId first
// became offline in this session, or null if currently online. Side-
// effecting: updates the module-level Map as transitions happen.
function recordOffline(nodeId: string, isOffline: boolean): number | null {
  if (!isOffline) {
    offlineSince.delete(nodeId);
    return null;
  }
  if (!offlineSince.has(nodeId)) {
    offlineSince.set(nodeId, Date.now());
  }
  return offlineSince.get(nodeId) ?? null;
}

interface OfflineBadgeProps {
  nodeId: string;
  isOffline: boolean;
  dark: boolean;
}

export function OfflineBadge({ nodeId, isOffline, dark }: Readonly<OfflineBadgeProps>) {
  const now = useTick();
  const since = recordOffline(nodeId, isOffline);
  if (!isOffline) return null;
  let text = "offline";
  if (since !== null) {
    const elapsedMs = now - since;
    if (elapsedMs >= 1_000) {
      text = `offline ${formatDuration(BigInt(Math.floor(elapsedMs / 1_000)))}`;
    }
  }
  return (
    <Badge variant="error" dark={dark} title="cluster has not heard from this node">
      {text}
    </Badge>
  );
}

// _resetOfflineTrackerForTest is exported only to give tests a clean
// slate between runs. Not part of the production API.
export function _resetOfflineTrackerForTest(): void {
  offlineSince.clear();
}
