import { useThemeClass } from "../../hooks/useThemeClass";
import { usePipelineBacklog } from "../../api/hooks/usePipelineBacklog";
import { useConfig } from "../../api/hooks";
import { idFromBytes } from "../../api/model/id";
import { resolveNodeName, buildNodeNameMap } from "../../utils/nodeNames";
import { protoToInstant, instantToDate, formatDateTimeShort } from "../../utils/temporal";
import { Badge } from "../Badge";
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

function NodeSegmentTable({
  title,
  colA,
  colB,
  rows,
  dark,
  nodeNames,
}: Readonly<{
  title: string;
  colA: { label: string; get: (row: NodeRow) => number };
  colB: { label: string; get: (row: NodeRow) => number };
  rows: NodeRow[];
  dark: boolean;
  nodeNames: Map<string, string>;
}>) {
  const c = useThemeClass(dark);
  const active = rows.filter((row) => colA.get(row) > 0 || colB.get(row) > 0);
  if (active.length === 0) return null;

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
            {active.map((row) => {
              const label = resolveNodeName(nodeNames, row.nodeId);
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
                  <td
                    className={`px-3 py-2.5 font-mono text-right tabular-nums whitespace-nowrap ${a > 0 ? c("text-text-bright", "text-light-text-bright") : c("text-text-muted", "text-light-text-muted")}`}
                  >
                    {formatCount(a)}
                  </td>
                  <td
                    className={`px-3 py-2.5 font-mono text-right tabular-nums whitespace-nowrap ${b > 0 ? c("text-text-bright", "text-light-text-bright") : c("text-text-muted", "text-light-text-muted")}`}
                  >
                    {formatCount(b)}
                  </td>
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
  nodeId: string;
  working: number;
  staged: number;
  head: number;
  preHead: number;
}

export function PipelineBacklogView({ vaultId, dark }: Readonly<PipelineBacklogViewProps>) {
  const c = useThemeClass(dark);
  const { data: backlog, isLoading } = usePipelineBacklog(vaultId);
  const { data: config } = useConfig();
  const nodeNames = buildNodeNameMap(config?.nodeConfigs ?? []);

  if (isLoading) {
    return (
      <div className={`px-4 py-3 border-b ${c("border-ink-border", "border-light-border")}`}>
        <LoadingPlaceholder dark={dark} />
      </div>
    );
  }
  if (!backlog) return null;

  const eligible = backlog.eligibleSegments;
  const registry = backlog.registrySegments;
  const backlogWarn = eligible > 100;

  const oldestEligible = backlog.oldestEligibleLastIngest
    ? formatDateTimeShort(instantToDate(protoToInstant(backlog.oldestEligibleLastIngest)))
    : "—";

  const leaderId = backlog.vaultCtlLeaderNodeId
    ? idFromBytes(backlog.vaultCtlLeaderNodeId)
    : undefined;
  const leaderLabel = leaderId
    ? resolveNodeName(nodeNames, leaderId)
    : null;

  const nodeRows: NodeRow[] = backlog.nodeSegments.map((ns) => ({
    nodeId: ns.nodeId.length > 0 ? idFromBytes(ns.nodeId) : "",
    working: ns.workingSegments,
    staged: ns.completedStagingSegments,
    head: ns.headSegments,
    preHead: ns.preHeadSegments,
  })).filter((row) => row.nodeId.length > 0);

  const border = c("border-ink-border", "border-light-border");
  const rowBorder = c("border-ink-border-subtle", "border-light-border-subtle");

  return (
    <div
      className={`px-4 py-3 border-b ${c("border-ink-border bg-ink-well", "border-light-border bg-light-well")}`}
    >
      <div className="flex items-center gap-2 mb-3">
        <span
          className={`text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Pipeline backlog
        </span>
        {backlog.connectedNodeIsVaultCtlLeader && (
          <Badge variant="info" dark={dark}>planner here</Badge>
        )}
        {leaderLabel && !backlog.connectedNodeIsVaultCtlLeader && (
          <Badge variant="muted" dark={dark} title="vault-ctl leader">
            planner: {leaderLabel}
          </Badge>
        )}
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

      <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4 items-start">
        <NodeSegmentTable
          title="Segmentation (origins)"
          colA={{ label: "Working", get: (r) => r.working }}
          colB={{ label: "Staged", get: (r) => r.staged }}
          rows={nodeRows}
          dark={dark}
          nodeNames={nodeNames}
        />

        <NodeSegmentTable
          title="Distribution (homes)"
          colA={{ label: "Head", get: (r) => r.head }}
          colB={{ label: "Pre-head", get: (r) => r.preHead }}
          rows={nodeRows}
          dark={dark}
          nodeNames={nodeNames}
        />
      </div>
    </div>
  );
}
