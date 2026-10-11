import { encode } from "../../api/glid";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useThemeClass } from "../../hooks/useThemeClass";
import { HelpLink } from "../HelpLink";
import { FormField } from "./FormField";
import { Checkbox } from "./Checkbox";

export const NODE_ASSIGNMENT_HELP_REF = "ingestion#node-assignment";

/**
 * Multi-select node picker for ingesters. Renders a list of checkboxes,
 * one per cluster node, plus a top-level "All nodes" checkbox.
 *
 * The "All nodes" checkbox is **NOT a UI shortcut for "check all current
 * nodes"** — that would bake a snapshot of cluster membership into the
 * config, and new joiners would silently drop out of the assignment. It
 * controls a real `allNodes` field on IngesterConfig that the backend
 * dispatcher re-evaluates on every cluster-membership change. When
 * `allNodes` is on, the per-node checkboxes are visually disabled and
 * the underlying `value` list is preserved (so unchecking "All nodes"
 * restores the previous selection).
 *
 * Hidden in single-node mode.
 */
export function NodeMultiSelect({
  value,
  allNodes,
  onValueChange,
  onAllNodesChange,
  dark,
}: Readonly<{
  value: string[];
  allNodes: boolean;
  onValueChange: (nodeIds: string[]) => void;
  onAllNodesChange: (allNodes: boolean) => void;
  dark: boolean;
}>) {
  const { data: clusterStatus } = useClusterStatus();
  const c = useThemeClass(dark);

  if (!clusterStatus?.clusterEnabled) return null;

  const nodes = clusterStatus.nodes;
  if (nodes.length === 0) return null;

  const sorted = nodes
    .map((n) => ({ id: encode(n.id), label: n.name || encode(n.id) }))
    .sort((a, b) => a.label.localeCompare(b.label));

  const selected = new Set(value);

  const toggle = (nodeId: string) => {
    if (selected.has(nodeId)) {
      onValueChange(value.filter((id) => id !== nodeId));
    } else {
      onValueChange([...value, nodeId]);
    }
  };

  // The dispatcher reads an empty list as "every node"; the picker must
  // not let that read as "no node".
  const nothingSelected = !allNodes && value.length === 0;

  return (
    <FormField label="Nodes" helpRef={NODE_ASSIGNMENT_HELP_REF} dark={dark}>
      <div className={`flex flex-col gap-1.5 px-2 py-1.5 rounded border ${c(
        "border-ink-border bg-ink-surface",
        "border-light-border bg-light-surface",
      )}`}>
        <Checkbox
          checked={allNodes}
          onChange={onAllNodesChange}
          label="All nodes — including nodes that join later"
          dark={dark}
        />
        <div className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`} />
        {sorted.map((node) => (
          <Checkbox
            key={node.id}
            checked={selected.has(node.id)}
            onChange={() => toggle(node.id)}
            label={node.label}
            dark={dark}
            disabled={allNodes}
          />
        ))}
      </div>
      {nothingSelected && (
        <div role="note" className={`text-[0.85em] leading-relaxed ${c("text-text-normal", "text-light-text-normal")}`}>
          No node selected — the ingester runs on every node, the same as All nodes.{" "}
          <HelpLink helpRef={NODE_ASSIGNMENT_HELP_REF} className="text-copper">Node assignment</HelpLink>
        </div>
      )}
    </FormField>
  );
}
