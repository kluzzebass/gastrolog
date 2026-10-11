import { encode } from "../../api/glid";
import { type EntityID, idFromBytes } from "../../api/model/id";
import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useVaults, useStorages, useIngesters, useNodeRegistry } from "../../api/hooks";
import { useWatchJobs } from "../../api/hooks";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useConfig } from "../../api/hooks/useSystem";
import { useSettings } from "../../api/hooks/useSettings";
import { toastError } from "../Toast";
import { VaultCard } from "./VaultCard";
import { StorageCard } from "./StorageCard";
import { IngesterCard } from "./IngesterCard";
import { JobCard, ScheduledJobsTable } from "./JobCard";
import { HelpButton } from "../HelpButton";
import { SystemStatsView } from "./SystemStatsView";
import { PeerBytesSection } from "./PeerBytesSection";
import { StatsFreshnessCue } from "./StatsFreshness";
import { Badge } from "../Badge";

interface NodeDetailPaneProps {
  nodeId: string;
  dark: boolean;
  onOpenSettings?: (tab: string, entityName?: string) => void;
  onNavigate?: (param: string) => void;
}

export function NodeDetailPane({ nodeId, dark, onOpenSettings, onNavigate }: Readonly<NodeDetailPaneProps>) {
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
  const { data: allStorages } = useStorages();
  const allIngesters = useIngesters();
  const { jobs, connected, reconnecting } = useWatchJobs({ onError: toastError });
  // Until the job stream delivers its first snapshot an empty list means
  // "not yet known", not "none" — the same condition the Jobs view waits on.
  const jobsLoading = !connected && !reconnecting && jobs.length === 0;

  const nscs = config?.nodeStorageConfigs ?? [];
  const vaults = allVaults.filter((v) => v.isOn(nodeIdTyped, nscs, registry.localNodeId));
  // Join on StorageState.node_id — the stable key — never nodeName, which
  // collides on rename races or duplicate names.
  const storages = allStorages.filter((s) => s.nodeId === nodeIdTyped);
  const ingesters = allIngesters.filter((i) => i.isEligibleOn(nodeIdTyped));
  const nodeJobs = jobs.filter((j) => (j.nodeId || registry.localNodeId) === nodeIdTyped);
  const tasks = nodeJobs.filter((j) => j.isTask);
  const scheduled = nodeJobs.filter((j) => j.isScheduled);

  // Expanded states per section (multi-expand).
  const [expandedVaults, setExpandedVaults] = useState<Record<string, boolean>>({});
  const [expandedStorages, setExpandedStorages] = useState<Record<string, boolean>>({});
  const [expandedIngesters, setExpandedIngesters] = useState<Record<string, boolean>>({});
  const [expandedJobs, setExpandedJobs] = useState<Record<string, boolean>>({});

  return (
    <div className="flex flex-col gap-6">
      {/* System section */}
      <Section title="System" dark={dark} helpRef="inspector-system">
        <SystemStatsView
          nodeStats={nodeInfo?.stats ?? null}
          lastSeen={nodeInfo?.lastSeen}
          receivedAt={node?.statsReceivedAt}
          ttlMs={registry.statsTtlMs}
          dark={dark}
        />
      </Section>

      {/* Vaults section */}
      <Section title="Vaults" dark={dark} helpRef="inspector-vaults">
        {vaults.length === 0 ? (
          <EmptyMessage dark={dark}>No vaults placed on this node.</EmptyMessage>
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

      {/* Storages section */}
      <Section title="Storages" dark={dark} helpRef="inspector-storages">
        {storages.length === 0 ? (
          <EmptyMessage dark={dark}>No file storages configured on this node.</EmptyMessage>
        ) : (
          <div className="flex flex-col gap-2">
            {[...storages]
              .sort((a, b) => a.displayLabel.localeCompare(b.displayLabel))
              .map((storage) => (
                <StorageCard
                  key={storage.id}
                  storage={storage}
                  dark={dark}
                  expanded={!!expandedStorages[storage.id]}
                  onToggle={() => setExpandedStorages((prev) => ({ ...prev, [storage.id]: !prev[storage.id] }))}
                  onOpenSettings={onOpenSettings ? () => onOpenSettings("storage", storage.displayLabel) : undefined}
                  onNavigate={onNavigate}
                />
              ))}
          </div>
        )}
      </Section>

      {/* Ingesters section */}
      <Section title="Ingesters" dark={dark} helpRef="inspector-ingesters">
        {ingesters.length === 0 ? (
          <EmptyMessage dark={dark}>No ingesters eligible to run on this node.</EmptyMessage>
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
      <Section
        title="Network"
        dark={dark}
        helpRef="inspector-system#network"
        trailing={<StatsFreshnessCue receivedAt={node?.statsReceivedAt} ttlMs={registry.statsTtlMs} dark={dark} />}
      >
        <PeerBytesSection
          viewNodeId={nodeIdTyped}
          nodeStats={nodeInfo?.stats ?? null}
          nodes={registry}
          dark={dark}
        />
      </Section>

      {/* Scheduled jobs section */}
      <Section title="Scheduled" dark={dark} helpRef="inspector-jobs#scheduled-jobs">
        <JobsSectionBody
          loading={jobsLoading}
          empty={scheduled.length === 0 ? "No scheduled jobs on this node." : null}
          dark={dark}
        >
          <ScheduledJobsTable jobs={scheduled} dark={dark} showNodeBadge={false} />
        </JobsSectionBody>
      </Section>

      {/* Tasks section */}
      <Section title="Tasks" dark={dark} helpRef="inspector-jobs#tasks">
        <JobsSectionBody
          loading={jobsLoading}
          empty={tasks.length === 0 ? "No tasks on this node." : null}
          dark={dark}
        >
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
        </JobsSectionBody>
      </Section>
    </div>
  );
}

// ---- Shared building blocks ----

function Section({
  title,
  dark,
  count,
  helpRef,
  trailing,
  children,
}: Readonly<{
  title: string;
  dark: boolean;
  count?: number;
  helpRef?: string;
  trailing?: React.ReactNode;
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
        {helpRef ? <HelpButton helpRef={helpRef} /> : null}
        {count !== undefined && count > 0 && (
          <Badge variant="muted" dark={dark}>{count}</Badge>
        )}
        {trailing && <span className="ml-auto">{trailing}</span>}
      </div>
      {children}
    </section>
  );
}

function JobsSectionBody({
  loading,
  empty,
  dark,
  children,
}: Readonly<{ loading: boolean; empty: string | null; dark: boolean; children: React.ReactNode }>) {
  if (loading) return <LoadingPlaceholder dark={dark} />;
  if (empty) return <EmptyMessage dark={dark}>{empty}</EmptyMessage>;
  return <>{children}</>;
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
