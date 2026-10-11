import { Badge } from "./Badge";
import { useNodeRegistry } from "../api/hooks";
import { decode } from "../api/glid";
import { idFromBytes } from "../api/model/id";
// eslint-disable-next-line no-restricted-imports -- no ContributionReport model yet
import { type ContributionReport } from "../api/gen/gastrolog/v1/vault_pb";

export const PARTIAL_HELP_REF = "inspector#partial-results";

/** The badge text for a merge that `unreached` peers dropped out of. */
export function partialBadgeText(unreached: number): string {
  return `partial — ${unreached} ${unreached === 1 ? "node" : "nodes"} unreached`;
}

/**
 * DegradedPeersBadge surfaces a partial cross-node merge: when a fanned-out
 * inspector or query response could not reach every peer, the backend
 * attaches a ContributionReport naming the peers that dropped out. This
 * badge turns that into a single quiet pill whose tooltip names each
 * unreached node and why it fell out.
 *
 * Quiet-until-needed: renders nothing when the report is absent or empty
 * (every peer contributed).
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
    <Badge
      variant="warn"
      dark={dark}
      title={`Partial result — the figures leave out what these nodes hold:\n${title}`}
      helpRef={PARTIAL_HELP_REF}
    >
      {partialBadgeText(degraded.length)}
    </Badge>
  );
}
