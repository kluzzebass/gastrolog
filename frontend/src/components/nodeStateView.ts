import { NodeState } from "../api/model/node";
import type { BadgeVariant } from "./Badge";

export interface StateView {
  label: string;
  variant: BadgeVariant;
  description: string;
}

// Live and Unspecified have no view — the steady state should not add
// visual noise (AGENTS.md "Quiet until needed").
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
};

/** The badge view for a node state, or undefined for the states that show no badge. */
export function nodeStateView(state: NodeState): StateView | undefined {
  return viewByState[state];
}
