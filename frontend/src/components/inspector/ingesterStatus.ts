import type { IngesterStatusVariant } from "../../api/model/ingester";

export interface IngesterStatusBadgeSpec {
  text: string;
  title: string;
}

/** The header badge for an ingester: disabled, nowhere to run, or how many of its eligible nodes run it. */
export function ingesterStatusBadge(args: {
  enabled: boolean;
  selected: number;
  running: number;
  variant: IngesterStatusVariant;
}): IngesterStatusBadgeSpec {
  if (!args.enabled) {
    return { text: "disabled", title: "disabled — runs on no node; enable it in Settings" };
  }
  if (args.selected === 0) {
    return {
      text: "no eligible node",
      title: "no live node may run this ingester — pin a live node or allow all nodes",
    };
  }
  const text = `${String(args.running)}/${String(args.selected)} running`;
  switch (args.variant) {
    case "info":
      return { text, title: "running on every eligible node (running / eligible)" };
    case "error":
      return { text, title: "an eligible pinned node is unreachable (running / eligible)" };
    default:
      return { text, title: "running on some eligible nodes only (running / eligible) — the rest have not started it" };
  }
}

/** The hover text of one node chip in the Nodes section. */
export function nodeChipTitle(name: string, status: { alive: boolean; dead: boolean }): string {
  if (status.dead) return `${name}: unreachable — the cluster has not heard from it`;
  if (!status.alive) return `${name}: eligible but not running the ingester`;
  return `${name}: running the ingester`;
}
