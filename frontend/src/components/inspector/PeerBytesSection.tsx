/**
 * Outbound peer connection telemetry from NodeStats.peerConnections — per-lane,
 * per-group, with purpose labels and backend-derived rates/sparklines.
 */
import { useLayoutEffect, useRef, useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { formatBytes } from "../../utils";
// eslint-disable-next-line no-restricted-imports -- passthrough proto types from Node.stats
import type {
  NodeStats,
  PeerConnStat,
} from "../../api/gen/gastrolog/v1/cluster_pb";
import { asEntityID } from "../../api/model/id";
import type { NodeRegistry } from "../../api/hooks";

function rowKey(p: PeerConnStat): string {
  return `${p.peer}\0${p.lane}\0${p.groupId}\0${p.poolIndex}`;
}

function laneLabel(p: PeerConnStat): string {
  if (p.lane === "service" && p.poolIndex > 0) {
    return `service #${p.poolIndex}`;
  }
  return p.lane || "service";
}

/** Strip vault/…/ctl wrapper for display; full id stays in title. */
function groupDisplay(groupId: string): { label: string; title: string } {
  if (!groupId) {
    return { label: "—", title: "" };
  }
  if (groupId.startsWith("vault/") && groupId.endsWith("/ctl")) {
    const vaultId = groupId.slice("vault/".length, -"/ctl".length);
    return { label: vaultId, title: groupId };
  }
  return { label: groupId, title: groupId };
}

export interface PeerBytesSectionProps {
  readonly nodeStats: NodeStats | null | undefined;
  readonly nodes: NodeRegistry;
  readonly dark: boolean;
}

export function PeerBytesSection({
  nodeStats,
  nodes,
  dark,
}: PeerBytesSectionProps) {
  const c = useThemeClass(dark);

  const rows: readonly PeerConnStat[] = nodeStats?.peerConnections ?? [];

  if (rows.length === 0) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
      >
        No inter-node connections recorded for this node.
      </div>
    );
  }

  const filtered = [...rows]
    .filter((p) => nodes.byId.has(asEntityID(p.peer)))
    .sort((a, b) => {
      const na = nodes.nameOf(asEntityID(a.peer));
      const nb = nodes.nameOf(asEntityID(b.peer));
      if (na !== nb) {
        return na.localeCompare(nb);
      }
      const la = laneLabel(a);
      const lb = laneLabel(b);
      if (la !== lb) {
        return la.localeCompare(lb);
      }
      return a.groupId.localeCompare(b.groupId);
    });

  if (filtered.length === 0) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
      >
        No inter-node connections recorded for this node.
      </div>
    );
  }

  const th = `px-3 py-1.5 text-left font-medium ${c("text-text-muted", "text-light-text-muted")}`;
  const thMetric = `${th} text-right whitespace-nowrap`;
  const tdTruncateCell = `px-3 py-1.5 max-w-0 ${c("text-text-normal", "text-light-text-normal")}`;
  const tdPeer = `px-3 py-1.5 whitespace-nowrap ${c("text-text-bright", "text-light-text-bright")}`;
  const tdRate = `px-3 py-1.5 text-right whitespace-nowrap tabular-nums ${c("text-text-bright", "text-light-text-bright")}`;

  return (
    <div
      className={`rounded-md border overflow-x-auto ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    >
      <table className="w-full text-[0.8em] font-mono">
        <thead>
          <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
            <th className={`${th} whitespace-nowrap`}>Peer</th>
            <th className={`${th} whitespace-nowrap`}>Lane</th>
            <th className={th}>Group</th>
            <th className={th}>Purpose</th>
            <th className={thMetric}>Tx/s</th>
            <th className="px-3 py-1.5" aria-hidden="true" />
            <th className={thMetric}>Rx/s</th>
            <th className="px-3 py-1.5" aria-hidden="true" />
          </tr>
        </thead>
        <tbody>
          {filtered.map((p) => {
            const txSpark = p.txSpark;
            const rxSpark = p.rxSpark;
            const hasHistory = txSpark.length > 0 || rxSpark.length > 0;
            const purposes = p.purposes.length > 0 ? p.purposes.join(", ") : "—";
            const group = groupDisplay(p.groupId);
            return (
              <tr
                key={rowKey(p)}
                className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
              >
                <td className={tdPeer}>
                  {nodes.nameOf(asEntityID(p.peer))}
                </td>
                <td className={`px-3 py-1.5 whitespace-nowrap ${c("text-text-normal", "text-light-text-normal")}`}>
                  {laneLabel(p)}
                </td>
                <td className={tdTruncateCell}>
                  <TruncatedText
                    text={group.label}
                    hint={group.title || group.label}
                  />
                </td>
                <td className={tdTruncateCell}>
                  <TruncatedText
                    text={purposes}
                    className={c("text-text-muted", "text-light-text-muted")}
                  />
                </td>
                <td className={tdRate}>
                  {hasHistory
                    ? `${formatBytes(Math.round(p.txBytesPerSec))}/s`
                    : "—"}
                </td>
                <td className="px-3 py-1 whitespace-nowrap text-copper">
                  <Spark values={txSpark} />
                </td>
                <td className={tdRate}>
                  {hasHistory
                    ? `${formatBytes(Math.round(p.rxBytesPerSec))}/s`
                    : "—"}
                </td>
                <td className="px-3 py-1 whitespace-nowrap text-copper-dim">
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

/** Truncated single-line text; native title appears only when ellipsis is shown. */
function TruncatedText({
  text,
  hint,
  className,
}: Readonly<{
  text: string;
  hint?: string;
  className?: string;
}>) {
  const ref = useRef<HTMLSpanElement>(null);
  const [overflowed, setOverflowed] = useState(false);
  const tooltip = hint ?? text;

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) {
      return;
    }
    const check = () => {
      setOverflowed(el.scrollWidth > el.clientWidth);
    };
    check();
    const ro = new ResizeObserver(check);
    ro.observe(el);
    return () => ro.disconnect();
  }, [text, tooltip]);

  return (
    <span
      ref={ref}
      className={`block truncate ${className ?? ""}`}
      title={overflowed && tooltip !== "—" ? tooltip : undefined}
    >
      {text}
    </span>
  );
}

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
