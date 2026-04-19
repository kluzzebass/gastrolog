/**
 * Per-peer inter-node gRPC tx/rx for a single node. Reads cumulative counters
 * from NodeStats.peerBytes and renders backend-derived rates + sparkline
 * windows. Covers ALL cluster transport traffic —
 * Raft, broadcast, tier replication, query forwarding, chunk streaming,
 * drain, etc. See gastrolog-47u85.
 */
import { useThemeClass } from "../../hooks/useThemeClass";
import { formatBytes } from "../../utils";
import type {
  NodeStats,
  PeerBytesStat,
} from "../../api/gen/gastrolog/v1/cluster_pb";

// peerKey returns the peer identifier used as the state-map key. Uses the
// proto-defined peer string (node ID) directly.
function peerKey(p: PeerBytesStat): string {
  return p.peer;
}

export interface PeerBytesSectionProps {
  readonly nodeStats: NodeStats | null | undefined;
  readonly peerNameById: ReadonlyMap<string, string>;
  readonly dark: boolean;
}

export function PeerBytesSection({
  nodeStats,
  peerNameById,
  dark,
}: PeerBytesSectionProps) {
  const c = useThemeClass(dark);

  const peerBytes: readonly PeerBytesStat[] = nodeStats?.peerBytes ?? [];

  if (peerBytes.length === 0) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
      >
        No inter-node traffic recorded for this node.
      </div>
    );
  }

  const rows = [...peerBytes].sort((a, b) => {
    const na = peerNameById.get(a.peer) ?? a.peer;
    const nb = peerNameById.get(b.peer) ?? b.peer;
    return na.localeCompare(nb);
  });

  return (
    <div
      className={`rounded-md border overflow-hidden ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    >
      <table className="w-full text-[0.8em] font-mono">
        <thead>
          <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
            <th
              className={`px-3 py-1.5 text-left font-medium ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Peer
            </th>
            <th
              className={`px-3 py-1.5 text-right font-medium ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Tx/s
            </th>
            <th
              className={`px-3 py-1.5 w-16 ${c("text-text-muted", "text-light-text-muted")}`}
            ></th>
            <th
              className={`px-3 py-1.5 text-right font-medium ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Rx/s
            </th>
            <th
              className={`px-3 py-1.5 w-16 ${c("text-text-muted", "text-light-text-muted")}`}
            ></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((p) => {
            const k = peerKey(p);
            const name = peerNameById.get(p.peer) ?? p.peer;
            const tx = p.txBytesPerSec;
            const rx = p.rxBytesPerSec;
            const txSpark = p.txSpark;
            const rxSpark = p.rxSpark;
            const hasHistory = txSpark.length > 0 || rxSpark.length > 0;
            return (
              <tr
                key={k}
                className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
              >
                <td
                  className={`px-3 py-1.5 truncate ${c("text-text-bright", "text-light-text-bright")}`}
                >
                  {name}
                </td>
                <td
                  className={`px-3 py-1.5 text-right ${c("text-text-bright", "text-light-text-bright")}`}
                >
                  {hasHistory ? `${formatBytes(Math.round(tx))}/s` : "—"}
                </td>
                <td className="px-3 py-1 text-copper">
                  <Spark values={txSpark} />
                </td>
                <td
                  className={`px-3 py-1.5 text-right ${c("text-text-bright", "text-light-text-bright")}`}
                >
                  {hasHistory ? `${formatBytes(Math.round(rx))}/s` : "—"}
                </td>
                <td className="px-3 py-1 text-copper-dim">
                  <Spark values={rxSpark} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// Spark renders a minimal inline line chart. Fixed-size SVG; auto-scales
// Y-axis to the window's own max so quiet and busy peers each fill the
// band meaningfully. Color comes from the parent via currentColor —
// callers set a Tailwind text-* class on the wrapper to pick the hue.
function Spark({ values }: Readonly<{ values: number[] }>) {
  if (values.length < 2) {
    return <svg width="56" height="16" aria-hidden="true" />;
  }
  const w = 56;
  const h = 16;
  const max = Math.max(...values, 1);
  const step = w / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = i * step;
      const y = h - (v / max) * h;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg width={w} height={h} aria-hidden="true">
      <polyline
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        points={points}
      />
    </svg>
  );
}
