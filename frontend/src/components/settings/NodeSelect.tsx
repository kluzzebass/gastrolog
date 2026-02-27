import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { FormField, SelectInput } from "./FormField";

/**
 * Node selector dropdown for choosing which cluster node owns a vault or ingester.
 * Hidden in single-node mode. When empty string is selected, the backend
 * auto-assigns the local node.
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

  if (!clusterStatus?.clusterEnabled) return null;

  const nodes = clusterStatus.nodes ?? [];
  if (nodes.length === 0) return null;

  const options = [
    { value: "", label: "(auto — local node)" },
    ...nodes.map((n) => ({
      value: n.id,
      label: n.name || n.id,
    })),
  ];

  return (
    <FormField label="Node" dark={dark}>
      <SelectInput value={value} onChange={onChange} options={options} dark={dark} />
    </FormField>
  );
}
