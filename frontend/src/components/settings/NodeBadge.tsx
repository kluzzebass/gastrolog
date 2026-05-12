import { useNodeRegistry } from "../../api/hooks";
import type { EntityID } from "../../api/model/id";
import { Badge } from "../Badge";

export function NodeBadge({
  nodeId,
  dark,
}: Readonly<{ nodeId: EntityID; dark: boolean }>) {
  const registry = useNodeRegistry();

  if (!nodeId) return null;
  if (!registry.multiNode) return null;

  const isLocal = registry.isLocal(nodeId);
  const label = registry.nameOf(nodeId);

  return (
    <>
      {isLocal && <Badge variant="copper" dark={dark}>this node</Badge>}
      <Badge variant="muted" dark={dark}>{label}</Badge>
    </>
  );
}
