import { Badge } from "./Badge";
import { useNodeRegistry } from "../api/hooks";
import { decode } from "../api/glid";
import { idFromBytes } from "../api/model/id";
// eslint-disable-next-line no-restricted-imports -- no ContributionReport model yet (gastrolog-2e2qs follow-up)
import { type ContributionReport } from "../api/gen/gastrolog/v1/vault_pb";

/**
 * DegradedPeersBadge surfaces a partial cross-node merge: when a fanned-out
 * inspector or query response could not reach every peer, the backend
 * attaches a ContributionReport naming the peers that dropped out. This
 * badge turns that into a single quiet "partial" pill whose tooltip names
 * each degraded node and why it fell out.
 *
 * Quiet-until-needed: renders nothing when the report is absent or empty
 * (every peer contributed). See gastrolog-66zrj.
 */
export function DegradedPeersBadge({
  report,
  dark,
}: Readonly<{ report: ContributionReport | null | undefined; dark: boolean }>) {
  const registry = useNodeRegistry();
  const degraded = report?.degraded ?? [];
  if (degraded.length === 0) return null;

  const nameOf = (nodeId: string): string => {
    try {
      return registry.nameOf(idFromBytes(decode(nodeId)));
    } catch {
      return nodeId;
    }
  };

  const title = degraded
    .map((d) => `${nameOf(d.nodeId)}${d.reason ? `: ${d.reason}` : ""}`)
    .join("\n");

  return (
    <Badge variant="warn" dark={dark} title={`Partial result — did not reach:\n${title}`}>
      partial ({degraded.length})
    </Badge>
  );
}
