import type { NodeState, Timestamp } from "../api/model/node";
import { Badge } from "./Badge";
import { nodeStateView } from "./nodeStateView";
import { formatDuration } from "../utils/units";

export const NODE_STATES_HELP_REF = "clustering-nodes#node-states";

interface NodeStateBadgeProps {
  state: NodeState;
  stateSince?: Timestamp;
  dark: boolean;
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
  const view = nodeStateView(state);
  if (!view) return null;
  const duration = durationSince(stateSince);
  const text = duration ? `${view.label} ${duration}` : view.label;
  return (
    <Badge variant={view.variant} dark={dark} title={view.description} helpRef={NODE_STATES_HELP_REF}>
      {text}
    </Badge>
  );
}
