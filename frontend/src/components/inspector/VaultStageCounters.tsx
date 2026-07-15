import { useThemeClass } from "../../hooks/useThemeClass";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { formatRate } from "../../utils/units";
import { Spark } from "../Spark";
import { HelpButton } from "../HelpButton";
import {
  aggregateStageCounters,
  perNodeTitle,
  type StageMilestone,
} from "./stageCounters";

const GROUP_LABELS: Record<StageMilestone["group"], string> = {
  segments: "Segments",
  chunks: "Chunks",
  recovery: "Recovery & retention",
};

// VaultStageCountersSection surfaces the discrete pipeline stage-count
// milestones for one vault — the events operators previously grepped from
// cluster.log (segments completed/published/released, chunks
// planned/built/sealed, head purges, GLCB catch-up pulls, retention deletes).
// Cluster totals are shown by default; the per-node breakdown is on hover.
// Quiet until an event happens: milestones with a zero cluster total and no
// rate are omitted (gastrolog-4r784a).
export function VaultStageCountersSection({
  vaultId,
  dark,
}: Readonly<{ vaultId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: cluster } = useClusterStatus();
  const milestones = aggregateStageCounters(cluster?.nodes, vaultId);
  if (milestones.length === 0) return null;

  const brightMono = `font-mono text-right tabular-nums ${c("text-text-bright", "text-light-text-bright")}`;
  const mutedMono = `font-mono text-right tabular-nums ${c("text-text-muted", "text-light-text-muted")}`;
  const rowBorder = c("border-ink-border-subtle", "border-light-border-subtle");
  const grid = "grid grid-cols-[minmax(9rem,1fr)_5rem_4.5rem_5.5rem] items-center gap-x-3";

  // Preserve pipeline order but insert a group label whenever the group flips.
  // Precompute the boundary per row so render stays side-effect free (the React
  // compiler is enabled — no mutation during map).
  const showGroupFor = milestones.map(
    (m, i) => i === 0 || m.group !== milestones[i - 1]?.group,
  );

  return (
    <section className="flex flex-col gap-4">
      <h3
        className={`flex items-center gap-1.5 text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Pipeline stages
        <HelpButton topicId="inspector-pipeline-stages" />
      </h3>
      <div className={`rounded-lg border overflow-hidden ${c("border-ink-border", "border-light-border")}`}>
        <div
          className={`${grid} px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c("text-text-muted border-ink-border-subtle bg-ink-well", "text-light-text-muted border-light-border-subtle bg-light-well")}`}
        >
          <span>Milestone</span>
          <span className="text-right">Total</span>
          <span />
          <span className="text-right">Rate</span>
        </div>
        {milestones.map((m, i) => {
          const showGroup = showGroupFor[i];
          const hasRate = m.clusterInstantPerSec !== undefined;
          // The rate spark stays per-node (phase-skewed sums fabricate a
          // series no node observed): show the busiest node's spark.
          let topSpark: number[] = [];
          for (const r of m.perNodeRate ?? []) {
            const spark = r.rate?.spark ?? [];
            if (spark.length > topSpark.length) topSpark = [...spark];
          }
          const warn = m.key === "glcbPullsFailed" && m.total > 0;
          return (
            <div key={m.key}>
              {showGroup && (
                <div
                  className={`px-4 pt-2 pb-1 text-[0.65em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted/70", "text-light-text-muted/70")}`}
                >
                  {GROUP_LABELS[m.group]}
                </div>
              )}
              <div className={`${grid} px-4 py-1.5 text-[0.85em] border-b last:border-b-0 ${rowBorder}`} title={perNodeTitle(m)}>
                <span className={c("text-text-normal", "text-light-text-normal")}>{m.label}</span>
                <span className={warn ? "font-mono text-right tabular-nums text-severity-warn" : brightMono}>
                  {m.total.toLocaleString()}
                </span>
                <span className="text-copper">{hasRate && <Spark values={topSpark} />}</span>
                <span className={hasRate && (m.clusterInstantPerSec ?? 0) > 0 ? brightMono : mutedMono}>
                  {hasRate ? `${formatRate(m.clusterInstantPerSec ?? 0)}/s` : "—"}
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}
