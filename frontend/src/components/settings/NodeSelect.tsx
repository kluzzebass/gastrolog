import { useEffect } from "react";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { FormField, SelectInput } from "./FormField";

/**
 * Node selector dropdown for choosing which cluster node owns a vault or ingester.
 * Hidden in single-node mode. Auto-selects the local node when no value is set.
 */
export function NodeSelect({
  value,
  onChange,
  dark,
}: Readonly<{
  value: string;
  onChange: (nodeId: string) => void;
  dark: boolean;
}>) {
  const { data: clusterStatus } = useClusterStatus();

  const localNodeId = clusterStatus?.localNodeId ?? "";

  // Auto-select the local node when value is empty and we know who we are.
  useEffect(() => {
    if (!value && localNodeId) {
      onChange(localNodeId);
    }
  }, [value, localNodeId, onChange]);

  if (!clusterStatus?.clusterEnabled) return null;

  const nodes = clusterStatus.nodes ?? [];
  if (nodes.length === 0) return null;

  const options = nodes.map((n) => ({
    value: n.id,
    label: n.name || n.id,
  }));

  return (
    <FormField label="Node" dark={dark}>
      <SelectInput value={value} onChange={onChange} options={options} dark={dark} />
    </FormField>
  );
}
