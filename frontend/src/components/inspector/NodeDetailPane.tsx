import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useVaults, useIngesters } from "../../api/hooks";
import { useWatchJobs } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useConfig } from "../../api/hooks/useConfig";
import { useSettings } from "../../api/hooks/useSettings";
import { JobKind } from "../../api/gen/gastrolog/v1/job_pb";
import { toastError } from "../Toast";
import { VaultCard } from "./VaultCard";
import { IngesterCard } from "./IngesterCard";
import { JobCard, ScheduledJobsTable } from "./JobCard";
import { SystemStatsView } from "./SystemStatsView";
import { Badge } from "../Badge";

interface NodeDetailPaneProps {
  nodeId: string;
  dark: boolean;
  onOpenSettings?: (tab: string, entityName?: string) => void;
}

export function NodeDetailPane({ nodeId, dark, onOpenSettings }: Readonly<NodeDetailPaneProps>) {
  const { data: settingsData } = useSettings();
  const localNodeId = settingsData?.nodeId ?? "";

  const { data: cluster } = useClusterStatus();
  const { data: config } = useConfig();
  const nodeInfo = cluster?.nodes.find((n) => n.id === nodeId);

  // Build vault ID → sealed backing provider map from config.
  const cloudProviders = new Map<string, string>();
  if (config?.vaults) {
    for (const vc of config.vaults) {
      const backing = vc.params["sealed_backing"] ?? vc.params["provider"];
      if (backing) {
        cloudProviders.set(vc.id, backing);
      }
    }
  }

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
        <SystemStatsView
          nodeStats={nodeInfo?.stats ?? null}
          dark={dark}
        />
      </Section>

      {/* Vaults section */}
      <Section title="Vaults" dark={dark}>
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
                  cloudProvider={cloudProviders.get(vault.id)}
                  dark={dark}
                  expanded={expandedVault === vault.id}
                  onToggle={() => setExpandedVault(expandedVault === vault.id ? null : vault.id)}
                  showNodeBadge={false}
                  onOpenSettings={onOpenSettings ? () => onOpenSettings("vaults", vault.name || vault.id) : undefined}
                />
              ))}
          </div>
        )}
      </Section>

      {/* Ingesters section */}
      <Section title="Ingesters" dark={dark}>
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
                onOpenSettings={onOpenSettings ? () => onOpenSettings("ingesters", ing.name || ing.id) : undefined}
              />
            ))}
          </div>
        )}
      </Section>

      {/* Scheduled jobs section */}
      <Section title="Scheduled" dark={dark}>
        {scheduled.length === 0 ? (
          <EmptyMessage dark={dark}>No scheduled jobs on this node.</EmptyMessage>
        ) : (
          <ScheduledJobsTable jobs={scheduled} dark={dark} showNodeBadge={false} />
        )}
      </Section>

      {/* Tasks section */}
      <Section title="Tasks" dark={dark}>
        {tasks.length === 0 ? (
          <EmptyMessage dark={dark}>No tasks on this node.</EmptyMessage>
        ) : (
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
