import { useThemeClass } from "../../hooks/useThemeClass";
import { usePipelineBacklog, usePipelineBacklogContribution, useNodeRegistry } from "../../api/hooks";
import { DegradedPeersBadge } from "../DegradedPeersBadge";
import { idFromBytes, type EntityID } from "../../api/model/id";
import { protoToInstant, instantToDate, formatDateTimeShort } from "../../utils/temporal";
import { formatBytes } from "../../utils/units";
import { LoadingPlaceholder } from "../LoadingPlaceholder";

interface PipelineBacklogViewProps {
  vaultId: string;
  dark: boolean;
}

function formatCount(n: number | bigint | undefined): string {
  if (n === undefined) return "—";
  const num = Number(n);
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
  return num.toLocaleString();
}

function formatCountAndBytes(count: number, bytes: bigint): string {
  return `${formatCount(count)} (${formatBytes(bytes)})`;
}

function protoBytes(n: bigint | undefined): bigint {
  return n ?? 0n;
}

const thClass = (c: (d: string, l: string) => string) =>
  `px-3 py-2 text-left text-[0.7em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c(
    "text-text-muted border-ink-border-subtle bg-ink-well",
    "text-light-text-muted border-light-border-subtle bg-light-well",
  )}`;

const tdClass = (c: (d: string, l: string) => string, warn?: boolean) => {
  let color = c("text-text-bright", "text-light-text-bright");
  if (warn) color = "text-severity-warn";
  return `px-3 py-2.5 font-mono font-semibold tabular-nums whitespace-nowrap ${color}`;
};

const tdMutedClass = (c: (d: string, l: string) => string, active: boolean) =>
  `px-3 py-2.5 font-mono text-right tabular-nums whitespace-nowrap text-[0.85em] ${
    active ? c("text-text-bright", "text-light-text-bright") : c("text-text-muted", "text-light-text-muted")
  }`;

function ClusterStagingTable({
  rows,
  dark,
}: Readonly<{
  rows: { area: string; count: number; bytes: bigint }[];
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const border = c("border-ink-border", "border-light-border");
  const rowBorder = c("border-ink-border-subtle", "border-light-border-subtle");

  return (
    <div className={`rounded-lg border overflow-x-auto ${border}`}>
      <table className="w-full border-collapse">
        <thead>
          <tr className={`border-b ${rowBorder}`}>
            <th className={thClass(c)}>Area</th>
            <th className={`${thClass(c)} text-right`}>Segments</th>
            <th className={`${thClass(c)} text-right`}>Bytes</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.area} className={`border-b last:border-b-0 ${rowBorder} text-[0.85em]`}>
              <td className={`px-3 py-2.5 font-mono whitespace-nowrap ${c("text-text-bright", "text-light-text-bright")}`}>
                {row.area}
              </td>
              <td className={tdMutedClass(c, row.count > 0)}>{formatCount(row.count)}</td>
              <td className={tdMutedClass(c, row.bytes > 0n)}>{formatBytes(row.bytes)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function NodeSegmentTable({
  title,
  colA,
  colB,
  rows,
  dark,
  nodeNameOf,
}: Readonly<{
  title: string;
  colA: { label: string; get: (row: NodeRow) => { count: number; bytes: bigint } };
  colB: { label: string; get: (row: NodeRow) => { count: number; bytes: bigint } };
  rows: NodeRow[];
  dark: boolean;
  nodeNameOf: (id: EntityID) => string;
}>) {
  const c = useThemeClass(dark);
  if (rows.length === 0) return null;

  const border = c("border-ink-border", "border-light-border");
  const rowBorder = c("border-ink-border-subtle", "border-light-border-subtle");

  return (
    <div className="min-w-0">
      <h3
        className={`text-[0.75em] font-medium uppercase tracking-[0.15em] mb-2 whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {title}
      </h3>
      <div className={`rounded-lg border overflow-x-auto ${border}`}>
        <table className="w-full border-collapse">
          <thead>
            <tr className={`border-b ${rowBorder}`}>
              <th className={thClass(c)}>Node</th>
              <th className={`${thClass(c)} text-right`}>{colA.label}</th>
              <th className={`${thClass(c)} text-right`}>{colB.label}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const label = nodeNameOf(row.nodeId);
              const a = colA.get(row);
              const b = colB.get(row);
              return (
                <tr key={row.nodeId} className={`border-b last:border-b-0 ${rowBorder} text-[0.85em]`}>
                  <td
                    className={`px-3 py-2.5 font-mono max-w-40 truncate whitespace-nowrap ${c("text-text-bright", "text-light-text-bright")}`}
                    title={row.nodeId}
                  >
                    {label}
                  </td>
                  <td className={tdMutedClass(c, a.count > 0)}>{formatCountAndBytes(a.count, a.bytes)}</td>
                  <td className={tdMutedClass(c, b.count > 0)}>{formatCountAndBytes(b.count, b.bytes)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

interface NodeRow {
  nodeId: EntityID;
  working: number;
  staged: number;
  head: number;
  preHead: number;
  workingBytes: bigint;
  stagedBytes: bigint;
  headBytes: bigint;
  preHeadBytes: bigint;
}

export function PipelineBacklogView({ vaultId, dark }: Readonly<PipelineBacklogViewProps>) {
  const c = useThemeClass(dark);
  const { data: backlog, isLoading } = usePipelineBacklog(vaultId);
  const contribution = usePipelineBacklogContribution(vaultId);
  const nodes = useNodeRegistry();

  if (isLoading) {
    return <LoadingPlaceholder dark={dark} />;
  }
  if (!backlog) return null;

  const eligible = backlog.eligibleSegments;
  const registry = backlog.registrySegments;
  const backlogWarn = eligible > 100;

  const oldestEligible = backlog.oldestEligibleLastIngest
    ? formatDateTimeShort(instantToDate(protoToInstant(backlog.oldestEligibleLastIngest)))
    : "—";

  const nodeRows: NodeRow[] = backlog.nodeSegments.flatMap((ns) => {
    if (ns.nodeId.length === 0) return [];
    return [
      {
        nodeId: idFromBytes(ns.nodeId),
        working: ns.workingSegments,
        staged: ns.completedStagingSegments,
        head: ns.headSegments,
        preHead: ns.preHeadSegments,
        workingBytes: protoBytes(ns.workingBytes),
        stagedBytes: protoBytes(ns.completedStagingBytes),
        headBytes: protoBytes(ns.headBytes),
        preHeadBytes: protoBytes(ns.preHeadBytes),
      },
    ];
  });

  const clusterStaging = [
    { area: "working", count: backlog.workingSegments, bytes: protoBytes(backlog.workingBytes) },
    { area: "completed", count: backlog.completedStagingSegments, bytes: protoBytes(backlog.completedStagingBytes) },
    { area: "pre-head", count: backlog.preHeadSegments, bytes: protoBytes(backlog.preHeadBytes) },
    { area: "head", count: backlog.headSegments, bytes: protoBytes(backlog.headBytes) },
  ];

  const border = c("border-ink-border", "border-light-border");
  const rowBorder = c("border-ink-border-subtle", "border-light-border-subtle");

  return (
    <section className="flex flex-col gap-4">
      <div className="flex items-center gap-2">
        <h3
          className={`text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Pipeline backlog
        </h3>
        <DegradedPeersBadge report={contribution} dark={dark} />
      </div>

      <div className={`rounded-lg border overflow-x-auto ${border}`}>
        <table className="w-full border-collapse">
          <thead>
            <tr className={`border-b ${rowBorder}`}>
              <th className={thClass(c)}>Registry</th>
              <th className={thClass(c)}>Registry records</th>
              <th className={thClass(c)}>Open manifest</th>
              <th className={thClass(c)}>Oldest unchunked ingest</th>
            </tr>
          </thead>
          <tbody>
            <tr className="text-[1.05em]">
              <td className={tdClass(c, backlogWarn)}>
                {`${formatCount(eligible)} / ${formatCount(registry)}`}
              </td>
              <td className={tdClass(c)}>{formatCount(backlog.registryRecords)}</td>
              <td className={tdClass(c)}>
                {`${formatCount(backlog.openManifestRefs)} refs · ${formatCount(backlog.openManifestRecords)} rec`}
              </td>
              <td className={tdClass(c, backlogWarn && oldestEligible !== "—")}>{oldestEligible}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="flex flex-col gap-2">
        <h3
          className={`text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Segment staging (cluster total)
        </h3>
        <ClusterStagingTable rows={clusterStaging} dark={dark} />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 items-start">
        <NodeSegmentTable
          title="Segmentation (origins)"
          colA={{ label: "Working", get: (r) => ({ count: r.working, bytes: r.workingBytes }) }}
          colB={{ label: "Completed", get: (r) => ({ count: r.staged, bytes: r.stagedBytes }) }}
          rows={nodeRows}
          dark={dark}
          nodeNameOf={nodes.nameOf}
        />

        <NodeSegmentTable
          title="Distribution (homes)"
          colA={{ label: "Head", get: (r) => ({ count: r.head, bytes: r.headBytes }) }}
          colB={{ label: "Pre-head", get: (r) => ({ count: r.preHead, bytes: r.preHeadBytes }) }}
          rows={nodeRows}
          dark={dark}
          nodeNameOf={nodes.nameOf}
        />
      </div>
    </section>
  );
}
