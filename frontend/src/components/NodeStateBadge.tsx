import { NodeState, type Timestamp } from "../api/model/node";
import { Badge, type BadgeVariant } from "./Badge";
import { formatDuration } from "../utils/units";

export const NODE_STATES_HELP_REF = "clustering-nodes#node-states";

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
// state should not add visual noise (AGENTS.md "Quiet until needed").
// Maintenance gets `info` rather than `muted` so operator intent is
// visible without competing with the warn tone reserved for problems.
const viewByState: Partial<Record<NodeState, StateView>> = {
  [NodeState.UNREACHABLE]: {
    label: "unreachable",
    variant: "warn",
    description: "no Raft contact from this node — check the process and its network; placements wait for it",
  },
  [NodeState.MAINTENANCE]: {
    label: "maintenance",
    variant: "info",
    description: "operator-declared absence — the cluster expects it back and keeps its placements",
  },
  [NodeState.DRAINING]: {
    label: "draining",
    variant: "info",
    description: "chunks are being transferred off this node ahead of its removal",
  },
  [NodeState.DECOMMISSIONING]: {
    label: "decommissioning",
    variant: "warn",
    description: "being removed from the cluster — it stops receiving placements",
  },
};

/** The badge view for a node state, or undefined for the states that show no badge. */
export function nodeStateView(state: NodeState): StateView | undefined {
  return viewByState[state];
}

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
    <Badge variant={view.variant} dark={dark} title={view.description} helpRef={NODE_STATES_HELP_REF}>
      {text}
    </Badge>
  );
}
