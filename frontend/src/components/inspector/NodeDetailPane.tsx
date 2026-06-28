import { encode } from "../../api/glid";
import { type EntityID, idFromBytes } from "../../api/model/id";
import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useVaults, useIngesters, useNodeRegistry } from "../../api/hooks";
import { useWatchJobs } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useConfig } from "../../api/hooks/useSystem";
import { useSettings } from "../../api/hooks/useSettings";
import { toastError } from "../Toast";
import { VaultCard } from "./VaultCard";
import { IngesterCard } from "./IngesterCard";
import { JobCard, ScheduledJobsTable } from "./JobCard";
import { SystemStatsView } from "./SystemStatsView";
import { PeerBytesSection } from "./PeerBytesSection";
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
  const registry = useNodeRegistry();
  const nodeIdTyped = nodeId as EntityID;
  const node = registry.byId.get(nodeIdTyped) ?? null;
  const nodeInfo = node?.cluster ?? null;
  const liveNodeIds: ReadonlySet<EntityID> = new Set(registry.all.filter((n) => n.isLive).map((n) => n.id));

  // Data for all entity types, filtered by this node.
  const { data: allVaults } = useVaults();
  const allIngesters = useIngesters();
  const { jobs } = useWatchJobs({ onError: toastError });

  const nscs = config?.nodeStorageConfigs ?? [];
  const vaults = allVaults.filter((v) => v.isOn(nodeIdTyped, nscs, registry.localNodeId));
  const ingesters = allIngesters.filter((i) => i.isEligibleOn(nodeIdTyped));
  const nodeJobs = jobs.filter((j) => (j.nodeId || registry.localNodeId) === nodeIdTyped);
  const tasks = nodeJobs.filter((j) => j.isTask);
  const scheduled = nodeJobs.filter((j) => j.isScheduled);

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
              .sort((a, b) => a.displayLabel.localeCompare(b.displayLabel))
              .map((vault) => (
                <VaultCard
                  key={vault.id}
                  vault={vault}
                  dark={dark}
                  expanded={!!expandedVaults[vault.id]}
                  onToggle={() => setExpandedVaults((prev) => ({ ...prev, [vault.id]: !prev[vault.id] }))}
                  onOpenSettings={onOpenSettings ? () => onOpenSettings("vaults", vault.displayLabel) : undefined}
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
                liveNodeIds={liveNodeIds}
                dark={dark}
                expanded={!!expandedIngesters[ing.id]}
                onToggle={() => setExpandedIngesters((prev) => ({ ...prev, [ing.id]: !prev[ing.id] }))}
                showNodeBadge={false}
                onOpenSettings={onOpenSettings ? () => onOpenSettings("ingesters", ing.displayLabel) : undefined}
              />
            ))}
          </div>
        )}
      </Section>

      {/* Network — per-link traffic (both peers' lanes merged). */}
      <Section title="Network" dark={dark}>
        <PeerBytesSection
          viewNodeId={nodeIdTyped}
          nodeStats={nodeInfo?.stats ?? null}
          nodes={registry}
          dark={dark}
        />
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
                expanded={!!expandedJobs[job.id]}
                onToggle={() => setExpandedJobs((prev) => ({ ...prev, [job.id]: !prev[job.id] }))}
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
