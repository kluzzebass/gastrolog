import { NodeState, type Timestamp } from "../api/model/node";
import { Badge, type BadgeVariant } from "./Badge";
import { formatDuration } from "../utils/units";

interface NodeStateBadgeProps {
  state: NodeState;
  stateSince?: Timestamp;
  dark: boolean;
}

interface StateView {
  label: string;
  variant: BadgeVariant;
  description: string;
}

// Live and Unspecified intentionally return null below — the steady
// state should not add visual noise (CLAUDE.md "Quiet until needed").
// Maintenance gets `info` rather than `muted` so operator intent is
// visible without competing with the warn tone reserved for problems.
const viewByState: Partial<Record<NodeState, StateView>> = {
  [NodeState.UNREACHABLE]: {
    label: "unreachable",
    variant: "warn",
    description: "cluster has not heard from this node",
  },
  [NodeState.MAINTENANCE]: {
    label: "maintenance",
    variant: "info",
    description: "operator-declared absence",
  },
  [NodeState.DRAINING]: {
    label: "draining",
    variant: "info",
    description: "transferring chunks off this node",
  },
  [NodeState.DECOMMISSIONING]: {
    label: "decommissioning",
    variant: "warn",
    description: "node is being removed from the cluster",
  },
};

function durationSince(ts: Timestamp | undefined): string {
  if (!ts) return "";
  const secs = ts.seconds;
  if (secs === 0n) return "";
  const nowSecs = BigInt(Math.floor(Date.now() / 1000));
  const elapsed = nowSecs - secs;
  if (elapsed <= 0n) return "";
  return formatDuration(elapsed);
}

export function NodeStateBadge({ state, stateSince, dark }: Readonly<NodeStateBadgeProps>) {
  const view = viewByState[state];
  if (!view) return null;
  const duration = durationSince(stateSince);
  const text = duration ? `${view.label} ${duration}` : view.label;
  return (
    <Badge variant={view.variant} dark={dark} title={view.description}>
      {text}
    </Badge>
  );
}
