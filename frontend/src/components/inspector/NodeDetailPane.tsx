import { encode } from "../../api/glid";
import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useVaults, useIngesters } from "../../api/hooks";
import { useWatchJobs } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useConfig } from "../../api/hooks/useSystem";
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
  const localNodeId = settingsData?.nodeId ? encode(settingsData.nodeId) : "";

  const { data: cluster } = useClusterStatus();
  const { data: config } = useConfig();
  const nodeInfo = cluster?.nodes.find((n) => encode(n.id) === nodeId);

  // Build vault ID → cloud tier type map from config tiers.
  const cloudProviders = new Map<string, string>();
  if (config) {
    for (const tier of config.tiers) {
      if (encode(tier.cloudServiceId) && encode(tier.vaultId)) {
        cloudProviders.set(encode(tier.vaultId), "cloud");
      }
    }
  }

  // Data for all entity types, filtered by this node.
  const { data: allVaults } = useVaults();
  const { data: allIngesters } = useIngesters();
  const { jobs } = useWatchJobs({ onError: toastError });

  const vaults = (allVaults ?? []).filter((v) => (encode(v.nodeId) || localNodeId) === nodeId);
  const ingesters = (allIngesters ?? []).filter((i) => i.nodeIds.length === 0 || i.nodeIds.some((n) => encode(n) === nodeId));
  const nodeJobs = jobs.filter((j) => (encode(j.nodeId) || localNodeId) === nodeId);
  const tasks = nodeJobs.filter((j) => j.kind === JobKind.TASK);
  const scheduled = nodeJobs.filter((j) => j.kind === JobKind.SCHEDULED);

  // Expanded states per section (multi-expand).
  const [expandedVaults, setExpandedVaults] = useState<Record<string, boolean>>({});
  const [expandedIngesters, setExpandedIngesters] = useState<Record<string, boolean>>({});
  const [expandedJobs, setExpandedJobs] = useState<Record<string, boolean>>({});

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
              .sort((a, b) => (a.name || encode(a.id)).localeCompare(b.name || encode(b.id)))
              .map((vault) => {
                const vid = encode(vault.id);
                return (
                <VaultCard
                  key={vid}
                  vault={vault}
                  cloudProvider={cloudProviders.get(vid)}
                  dark={dark}
                  expanded={!!expandedVaults[vid]}
                  onToggle={() => setExpandedVaults((prev) => ({ ...prev, [vid]: !prev[vid] }))}
                  onOpenSettings={onOpenSettings ? () => onOpenSettings("vaults", vault.name || vid) : undefined}
                />
                );
              })}
          </div>
        )}
      </Section>

      {/* Ingesters section */}
      <Section title="Ingesters" dark={dark}>
        {ingesters.length === 0 ? (
          <EmptyMessage dark={dark}>No ingesters on this node.</EmptyMessage>
        ) : (
          <div className="flex flex-col gap-2">
            {ingesters.map((ing) => {
              const iid = encode(ing.id);
              return (
              <IngesterCard
                key={iid}
                ingester={ing}
                dark={dark}
                expanded={!!expandedIngesters[iid]}
                onToggle={() => setExpandedIngesters((prev) => ({ ...prev, [iid]: !prev[iid] }))}
                showNodeBadge={false}
                onOpenSettings={onOpenSettings ? () => onOpenSettings("ingesters", ing.name || iid) : undefined}
              />
              );
            })}
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
            {tasks.map((job) => {
              const jid = encode(job.id);
              return (
              <JobCard
                key={jid}
                job={job}
                dark={dark}
                expanded={!!expandedJobs[jid]}
                onToggle={() => setExpandedJobs((prev) => ({ ...prev, [jid]: !prev[jid] }))}
                showNodeBadge={false}
              />
              );
            })}
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
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted", "text-light-text-muted")}`}
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
    <div className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
      {children}
    </div>
  );
}
