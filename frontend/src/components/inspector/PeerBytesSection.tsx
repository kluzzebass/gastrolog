/**
 * Cluster-port traffic per peer link — each lane merges this node's outbound
 * connection with the peer's outbound connection back (both directions).
 */
import { useThemeClass } from "../../hooks/useThemeClass";
import { formatBytes } from "../../utils";
import { useVaults } from "../../api/hooks";
// eslint-disable-next-line no-restricted-imports -- NodeStats passthrough from Node.stats gossip
import type { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { type EntityID } from "../../api/model/id";
import type { NodeRegistry } from "../../api/hooks";
import {
  mergeAllPeerTraffic,
  laneDetailText,
  type MergedPeerLane,
} from "./peerTrafficMerge";

function laneLabel(lane: string, poolIndex: number): string {
  if (lane === "service") {
    return `service #${poolIndex}`;
  }
  return lane || "service";
}

function rowKey(peerId: EntityID, lane: MergedPeerLane): string {
  return `${peerId}\0${lane.lane}\0${lane.groupId}\0${lane.poolIndex}`;
}

export interface PeerBytesSectionProps {
  readonly viewNodeId: EntityID;
  readonly nodeStats: NodeStats | null | undefined;
  readonly nodes: NodeRegistry;
  readonly dark: boolean;
}

export function PeerBytesSection({
  viewNodeId,
  nodeStats,
  nodes,
  dark,
}: PeerBytesSectionProps) {
  const c = useThemeClass(dark);
  const { data: vaults } = useVaults();
  const vaultNameOf = (vaultId: EntityID) =>
    vaults.find((v) => v.id === vaultId)?.displayLabel;
  const detailOpts = { vaultNameOf };

  const peerStatsById = new Map<EntityID, NodeStats | null | undefined>();
  for (const n of nodes.all) {
    peerStatsById.set(n.id, n.stats);
  }

  const merged = mergeAllPeerTraffic(viewNodeId, nodeStats, peerStatsById)
    .filter((m) => nodes.byId.has(m.peerId))
    .sort((a, b) =>
      nodes.nameOf(a.peerId).localeCompare(nodes.nameOf(b.peerId)),
    );

  if (merged.length === 0) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
      >
        No inter-node connections recorded for this node.
      </div>
    );
  }

  const th = `px-3 py-1.5 text-left font-medium ${c("text-text-muted", "text-light-text-muted")}`;
  const thMetric = `${th} w-0 text-right whitespace-nowrap`;
  const tdDetail = `px-3 py-1.5 w-full ${c("text-text-normal", "text-light-text-normal")}`;
  const tdPeer = `px-3 py-1.5 w-0 whitespace-nowrap ${c("text-text-bright", "text-light-text-bright")}`;
  const tdChild = `px-3 py-1.5 w-0 whitespace-nowrap pl-6 ${c("text-text-muted", "text-light-text-muted")}`;
  const tdMetric = `px-3 py-1.5 w-0 text-right whitespace-nowrap tabular-nums ${c("text-text-bright", "text-light-text-bright")}`;

  return (
    <div
      className={`rounded-md border overflow-x-auto ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    >
      <table className="w-full text-[0.8em] font-mono">
        <thead>
          <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
            <th className={`${th} w-0 whitespace-nowrap`}>Peer</th>
            <th className={`${th} w-full`}>Detail</th>
            <th className={thMetric}>Tx/s</th>
            <th className={thMetric}>Rx/s</th>
          </tr>
        </thead>
        <tbody>
          {merged.map((group) => (
            <PeerTrafficGroup
              key={group.peerId}
              peerName={nodes.nameOf(group.peerId)}
              peerId={group.peerId}
              total={group.total}
              lanes={group.lanes}
              c={c}
              tdPeer={tdPeer}
              tdChild={tdChild}
              tdDetail={tdDetail}
              tdMetric={tdMetric}
              detailOpts={detailOpts}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function PeerTrafficGroup({
  peerName,
  peerId,
  total,
  lanes,
  c,
  tdPeer,
  tdChild,
  tdDetail,
  tdMetric,
  detailOpts,
}: Readonly<{
  peerName: string;
  peerId: EntityID;
  total: MergedPeerLane;
  lanes: readonly MergedPeerLane[];
  c: (dark: string, light: string) => string;
  tdPeer: string;
  tdChild: string;
  tdDetail: string;
  tdMetric: string;
  detailOpts: { vaultNameOf: (vaultId: EntityID) => string | undefined };
}>) {
  const border = `border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`;

  return (
    <>
      <TrafficRow
        label={peerName}
        child={false}
        detail={laneDetailText(total, { isTotal: true, ...detailOpts })}
        txBytesPerSec={total.txBytesPerSec}
        rxBytesPerSec={total.rxBytesPerSec}
        txSpark={total.txSpark}
        rxSpark={total.rxSpark}
        border={border}
        tdPeer={tdPeer}
        tdChild={tdChild}
        tdDetail={tdDetail}
        tdMetric={tdMetric}
        c={c}
      />
      {lanes.map((lane) => (
          <TrafficRow
            key={rowKey(peerId, lane)}
            label={laneLabel(lane.lane, lane.poolIndex)}
            child
            detail={laneDetailText(lane, detailOpts)}
            txBytesPerSec={lane.txBytesPerSec}
            rxBytesPerSec={lane.rxBytesPerSec}
            txSpark={lane.txSpark}
            rxSpark={lane.rxSpark}
            border={border}
            tdPeer={tdPeer}
            tdChild={tdChild}
            tdDetail={tdDetail}
            tdMetric={tdMetric}
            c={c}
          />
      ))}
    </>
  );
}

function TrafficRow({
  label,
  child,
  detail,
  txBytesPerSec,
  rxBytesPerSec,
  txSpark,
  rxSpark,
  border,
  tdPeer,
  tdChild,
  tdDetail,
  tdMetric,
  c,
}: Readonly<{
  label: string;
  child: boolean;
  detail: { label: string; title: string };
  txBytesPerSec: number;
  rxBytesPerSec: number;
  txSpark: readonly number[];
  rxSpark: readonly number[];
  border: string;
  tdPeer: string;
  tdChild: string;
  tdDetail: string;
  tdMetric: string;
  c: (dark: string, light: string) => string;
}>) {
  const hasHistory = txSpark.length > 0 || rxSpark.length > 0;
  const detailClass = child
    ? `${tdDetail} ${c("text-text-muted", "text-light-text-muted")}`
    : tdDetail;
  const detailTitle =
    detail.title && detail.title !== detail.label ? detail.title : undefined;

  return (
    <tr className={border}>
      <td className={child ? tdChild : tdPeer}>{label}</td>
      <td className={detailClass} title={detailTitle}>
        {detail.label}
      </td>
      <td className={tdMetric}>
        <MetricCell
          rate={hasHistory ? `${formatBytes(Math.round(txBytesPerSec))}/s` : "—"}
          spark={txSpark}
          sparkClass="text-copper"
        />
      </td>
      <td className={tdMetric}>
        <MetricCell
          rate={hasHistory ? `${formatBytes(Math.round(rxBytesPerSec))}/s` : "—"}
          spark={rxSpark}
          sparkClass="text-copper-dim"
        />
      </td>
    </tr>
  );
}

function MetricCell({
  rate,
  spark,
  sparkClass,
}: Readonly<{
  rate: string;
  spark: readonly number[];
  sparkClass: string;
}>) {
  return (
    <div className="inline-flex items-center justify-end gap-2">
      <span>{rate}</span>
      <span className={sparkClass}>
        <Spark values={spark} />
      </span>
    </div>
  );
}

function Spark({ values }: Readonly<{ values: readonly number[] }>) {
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
