import { encode } from "../../api/glid";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useThemeClass } from "../../hooks/useThemeClass";
import { FormField } from "./FormField";
import { Checkbox } from "./Checkbox";

/**
 * Multi-select node picker for ingesters. Renders a list of checkboxes,
 * one per cluster node. Hidden in single-node mode.
 */
export function NodeMultiSelect({
  value,
  onChange,
  dark,
}: Readonly<{
  value: string[];
  onChange: (nodeIds: string[]) => void;
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
      onChange(value.filter((id) => id !== nodeId));
    } else {
      onChange([...value, nodeId]);
    }
  };

  const allSelected = sorted.every((n) => selected.has(n.id));

  const toggleAll = () => {
    if (allSelected) {
      onChange([]);
    } else {
      onChange(sorted.map((n) => n.id));
    }
  };

  return (
    <FormField label="Nodes" dark={dark}>
      <div className={`flex flex-col gap-1.5 px-2 py-1.5 rounded border ${c(
        "border-ink-border bg-ink-surface",
        "border-light-border bg-light-surface",
      )}`}>
        <Checkbox
          checked={allSelected}
          onChange={toggleAll}
          label="All nodes"
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
          />
        ))}
      </div>
    </FormField>
  );
}
