import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useVaults, useIngesters } from "../../api/hooks";
import { useWatchJobs } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useSettings } from "../../api/hooks/useConfig";
import { JobKind } from "../../api/gen/gastrolog/v1/job_pb";
import { toastError } from "../Toast";
import { VaultCard } from "./VaultCard";
import { IngesterCard } from "./IngesterCard";
import { JobCard, ScheduledJobsTable } from "./JobCard";
import { SystemStatsView, LocalSystemStats } from "./SystemStatsView";
import { Badge } from "../Badge";

interface NodeDetailPaneProps {
  nodeId: string;
  dark: boolean;
}

export function NodeDetailPane({ nodeId, dark }: Readonly<NodeDetailPaneProps>) {
  const { data: settingsData } = useSettings();
  const localNodeId = settingsData?.nodeId ?? "";
  const isLocal = nodeId === localNodeId;

  const { data: cluster } = useClusterStatus();
  const nodeInfo = cluster?.nodes.find((n) => n.id === nodeId);

  // Data for all entity types, filtered by this node.
  const { data: allVaults } = useVaults();
  const { data: allIngesters } = useIngesters();
  const { jobs } = useWatchJobs({ onError: toastError });

  const vaults = (allVaults ?? []).filter((v) => (v.nodeId || localNodeId) === nodeId);
  const ingesters = (allIngesters ?? []).filter((i) => (i.nodeId || localNodeId) === nodeId);
  const nodeJobs = jobs.filter((j) => (j.nodeId || localNodeId) === nodeId);
  const tasks = nodeJobs.filter((j) => j.kind === JobKind.TASK);
  const scheduled = nodeJobs.filter((j) => j.kind === JobKind.SCHEDULED);

  // Expanded states per section.
  const [expandedVault, setExpandedVault] = useState<string | null>(null);
  const [expandedIngester, setExpandedIngester] = useState<string | null>(null);
  const [expandedJob, setExpandedJob] = useState<string | null>(null);

  return (
    <div className="flex flex-col gap-6">
      {/* System section */}
      <Section title="System" dark={dark}>
        {isLocal ? (
          <LocalSystemStats dark={dark} />
        ) : (
          <SystemStatsView
            nodeStats={nodeInfo?.stats ?? null}
            dark={dark}
          />
        )}
      </Section>

      {/* Vaults section */}
      <Section title="Vaults" dark={dark} count={vaults.length}>
        {vaults.length === 0 ? (
          <EmptyMessage dark={dark}>No vaults on this node.</EmptyMessage>
        ) : (
          <div className="flex flex-col gap-2">
            {[...vaults]
              .sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id))
              .map((vault) => (
                <VaultCard
                  key={vault.id}
                  vault={vault}
                  dark={dark}
                  expanded={expandedVault === vault.id}
                  onToggle={() => setExpandedVault(expandedVault === vault.id ? null : vault.id)}
                  showNodeBadge={false}
                />
              ))}
          </div>
        )}
      </Section>

      {/* Ingesters section */}
      <Section title="Ingesters" dark={dark} count={ingesters.length}>
        {ingesters.length === 0 ? (
          <EmptyMessage dark={dark}>No ingesters on this node.</EmptyMessage>
        ) : (
          <div className="flex flex-col gap-2">
            {ingesters.map((ing) => (
              <IngesterCard
                key={ing.id}
                ingester={ing}
                dark={dark}
                expanded={expandedIngester === ing.id}
                onToggle={() => setExpandedIngester(expandedIngester === ing.id ? null : ing.id)}
                showNodeBadge={false}
              />
            ))}
          </div>
        )}
      </Section>

      {/* Jobs section */}
      <Section title="Jobs" dark={dark} count={tasks.length + scheduled.length}>
        {tasks.length === 0 && scheduled.length === 0 ? (
          <EmptyMessage dark={dark}>No jobs on this node.</EmptyMessage>
        ) : (
          <div className="flex flex-col gap-4">
            <ScheduledJobsTable jobs={scheduled} dark={dark} showNodeBadge={false} />
            {tasks.length > 0 && (
              <div className="flex flex-col gap-2">
                {tasks.map((job) => (
                  <JobCard
                    key={job.id}
                    job={job}
                    dark={dark}
                    expanded={expandedJob === job.id}
                    onToggle={() => setExpandedJob(expandedJob === job.id ? null : job.id)}
                    showNodeBadge={false}
                  />
                ))}
              </div>
            )}
          </div>
        )}
      </Section>
    </div>
  );
}

// ---- Shared building blocks ----

function Section({
  title,
  dark,
  count,
  children,
}: Readonly<{
  title: string;
  dark: boolean;
  count?: number;
  children: React.ReactNode;
}>) {
  const c = useThemeClass(dark);
  return (
    <section>
      <div className="flex items-center gap-2 mb-3">
        <h3
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          {title}
        </h3>
        {count !== undefined && count > 0 && (
          <Badge variant="muted" dark={dark}>{count}</Badge>
        )}
      </div>
      {children}
    </section>
  );
}

function EmptyMessage({
  dark,
  children,
}: Readonly<{ dark: boolean; children: React.ReactNode }>) {
  const c = useThemeClass(dark);
  return (
    <div className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
      {children}
    </div>
  );
}
