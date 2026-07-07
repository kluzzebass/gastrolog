import { encode } from "../../api/glid";
import { useState, type ReactNode } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";
import { useChunks, useIndexes, useValidateVault, useConfig, useArchiveChunk, useRestoreChunk } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useNodeRegistry } from "../../api/hooks";
import { usePipelineBacklog } from "../../api/hooks";
import { useToast } from "../Toast";
import { buildNodeNameMap, resolveNodeName } from "../../utils/nodeNames";
// eslint-disable-next-line no-restricted-imports -- no Chunk model yet (gastrolog-2e2qs follow-up)
import { ChunkState, type ChunkMeta } from "../../api/gen/gastrolog/v1/vault_pb";
import type { Vault } from "../../api/model/vault";
import { protoToInstant, instantToMs, instantToDate, formatDateTimeShort } from "../../utils/temporal";
import { formatBytes, formatRate } from "../../utils/units";
import { Spark } from "../Spark";
import { middleTruncate } from "../../utils/middleTruncate";
import { leaderNodeId, followerNodeIds } from "../../utils/placement";
import { Badge } from "../Badge";
import { BlobMark, SealPips } from "./SealPips";
import { computePips } from "./sealPipState";
import { CogIcon } from "../icons";
import { ExpandableCard } from "../settings/ExpandableCard";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { CrossLinkBadge } from "./CrossLinkBadge";
import { PipelineBacklogView } from "./PipelineBacklogView";

// chunkEndInstant returns the ingest/write end for display, omitting unset or
// sentinel epoch timestamps that proto encodes for zero-value Go times.
function chunkEndInstant(
  chunk: ChunkMeta,
  start: Date | undefined,
): Date | undefined {
  const endTs = chunk.ingestEnd ?? chunk.writeEnd;
  if (!endTs) return undefined;
  const end = instantToDate(protoToInstant(endTs));
  if (end.getFullYear() < 2000) return undefined;
  if (start && end.getTime() <= start.getTime()) return undefined;
  return end;
}

function chunkStartInstant(chunk: ChunkMeta): Date | undefined {
  const startTs = chunk.ingestStart ?? chunk.writeStart;
  if (!startTs) return undefined;
  const start = instantToDate(protoToInstant(startTs));
  if (start.getFullYear() < 2000) return undefined;
  return start;
}

// chunkLifecycleState maps the FSM overlay to the pip grammar's cluster-wide
// chunk lifecycle. Legacy entries without State fall back to the Sealed bool.
function chunkLifecycleState(chunk: ChunkMeta): "active" | "sealing" | "sealed" {
  if (chunk.sealed || chunk.state === ChunkState.SEALED) return "sealed";
  if (chunk.state === ChunkState.SEALING) return "sealing";
  return "active";
}

interface VaultCardProps {
  vault: Vault;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onOpenSettings?: () => void;
}

export function VaultCard({
  vault,
  dark,
  expanded,
  onToggle,
  onOpenSettings,
}: Readonly<VaultCardProps>) {
  // Use ListChunks data (fans out to leader nodes, authoritative per chunk).
  // ListVaults stats rely on periodic peer broadcasts and flicker.
  const { data: chunks } = useChunks(vault.id);
  const chunkCount = chunks?.length ?? 0;
  const recordCount = (chunks ?? []).reduce((sum, c) => sum + Number(c.recordCount), 0);

  return (
    <ExpandableCard
      key={vault.id}
      id={vault.displayLabel}
      typeBadge={vault.typeLabel}
      secondaryBadge={vault.isCloudBacked ? "cloud" : undefined}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          {!vault.enabled && (
            <Badge variant="warn" dark={dark}>disabled</Badge>
          )}
          <Badge variant="muted" dark={dark}>
            {chunkCount.toLocaleString()} chunks
          </Badge>
          <Badge variant="muted" dark={dark}>
            {recordCount.toLocaleString()} records
          </Badge>
          {onOpenSettings && (
            <CrossLinkBadge dark={dark} title="Open in Settings" onClick={onOpenSettings}>
              <CogIcon className="w-3 h-3" />
            </CrossLinkBadge>
          )}
        </span>
      }
    >
      <div className="flex flex-col gap-4 pt-2">
        <VaultLeaderSummary vaultId={vault.id} vaultTypeLabel={vault.typeLabel} dark={dark} />
        <VaultThroughputSection vaultId={vault.id} dark={dark} />
        <PipelineBacklogView vaultId={vault.id} dark={dark} />
        <ChunkList vaultId={vault.id} dark={dark} />
      </div>
    </ExpandableCard>
  );
}

// stageRow is one node's contribution to one pipeline stage.
interface StageRow {
  node: string;
  nodeId: string;
  recordsPerSec: number;
  bytesPerSec: number;
  spark: number[];
  extra?: { depth: number; cap: number; durablePerSec: number };
}

// idleNote explains WHY an idle node is idle — "caught up" is fine,
// "behind" while idle is a stall and warns. Absence of an explanation is
// how support cases are born.
interface IdleNote {
  text: string;
  warn: boolean;
}

// VaultThroughputSection is the three-stage pipeline readout for one vault,
// aggregated across every node's gossip-broadcast NodeStats: append (origin
// ingress), collected (segments arriving in head/ at the home), and sealed
// (records materialized into GLCBs). A downstream stage's rate falling away
// from its upstream is a pipeline stall in progress; the backlog panel below
// shows where the inventory stacks (gastrolog-10n6k8).
function VaultThroughputSection({
  vaultId,
  dark,
}: Readonly<{ vaultId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: cluster } = useClusterStatus();
  const { data: backlog } = usePipelineBacklog(vaultId);

  const append: StageRow[] = [];
  const collected: StageRow[] = [];
  const sealed: StageRow[] = [];
  for (const n of cluster?.nodes ?? []) {
    if (!n.stats) continue;
    for (const vs of n.stats.vaults) {
      if (encode(vs.id) !== vaultId) continue;
      const node = n.name || encode(n.id).slice(0, 8);
      const nodeId = encode(n.id);
      if (vs.appendQueueCapacity > 0) {
        append.push({
          node,
          nodeId,
          recordsPerSec: vs.appendRecords?.instantPerSec ?? 0,
          bytesPerSec: vs.appendBytes?.instantPerSec ?? 0,
          spark: vs.appendRecords?.spark ?? [],
          extra: {
            depth: vs.appendQueueDepth,
            cap: vs.appendQueueCapacity,
            durablePerSec: vs.appendDurable?.instantPerSec ?? 0,
          },
        });
      }
      if ((vs.collectedRecords?.spark.length ?? 0) > 0) {
        collected.push({
          node,
          nodeId,
          recordsPerSec: vs.collectedRecords?.instantPerSec ?? 0,
          bytesPerSec: vs.collectedBytes?.instantPerSec ?? 0,
          spark: vs.collectedRecords?.spark ?? [],
        });
      }
      if ((vs.sealedRecords?.spark.length ?? 0) > 0) {
        sealed.push({
          node,
          nodeId,
          recordsPerSec: vs.sealedRecords?.instantPerSec ?? 0,
          bytesPerSec: vs.sealedBytes?.instantPerSec ?? 0,
          spark: vs.sealedRecords?.spark ?? [],
        });
      }
    }
  }
  if (append.length === 0 && collected.length === 0 && sealed.length === 0) return null;

  // Idle explanations from the backlog data (same source as the panel below).
  const published = backlog?.registrySegments ?? 0;
  const headByNode = new Map<string, number>();
  for (const ns of backlog?.nodeSegments ?? []) {
    headByNode.set(encode(ns.nodeId), ns.headSegments);
  }
  const collectIdleNote = (r: StageRow): IdleNote => {
    const head = headByNode.get(r.nodeId);
    if (head === undefined || published === 0) return { text: "idle", warn: false };
    if (head >= published) return { text: "caught up", warn: false };
    return { text: `behind ${(published - head).toLocaleString()} segments`, warn: true };
  };
  const appendIdleNote = (): IdleNote => ({ text: "no ingest", warn: false });
  const eligible = backlog?.eligibleSegments ?? 0;
  const sealIdleNote = (): IdleNote => {
    if (eligible > 0) {
      return {
        text: `backlog: ${eligible.toLocaleString()} segments eligible`,
        warn: true,
      };
    }
    // A sealed manifest pending build with nothing else eligible reads as
    // "backlog: 0" — say what is actually happening instead.
    if (backlog?.sealedManifestPending) {
      return { text: "manifest awaiting build", warn: true };
    }
    return { text: "up to date", warn: false };
  };

  // Answers "why is COLLECTED 4x APPEND?" inline instead of via tooltip:
  // replicated stages count each record once PER HOME, so their Σ runs at
  // append × home-count when healthy — and ahead of it when a rejoined
  // node backfills or backlog drains (gastrolog-4deb9e).
  const totalAppend = append.reduce((s, r) => s + r.recordsPerSec, 0);
  const replicationNote = (rows: StageRow[]): IdleNote | undefined => {
    if (rows.length < 2) return undefined;
    const total = rows.reduce((s, r) => s + r.recordsPerSec, 0);
    if (total <= 0) return undefined;
    if (totalAppend <= 0) return { text: "catch-up", warn: false };
    const ratio = total / (totalAppend * rows.length);
    return ratio > 1.25
      ? { text: `×${rows.length} replication + catch-up`, warn: false }
      : { text: `×${rows.length} replication`, warn: false };
  };

  // Fixed grid template shared by every row (header, stage totals, node
  // rows) so changing number widths never shift columns horizontally.
  // STAGE ("COLLECTED") and NODE ("Σ 4 homes") have fixed-width content, so
  // they get fixed columns; STATUS is the only prose column and takes all
  // spare width — it was clipping while NODE flexed (gastrolog-4deb9e).
  const gridCols = "grid grid-cols-[5rem_5.5rem_4.5rem_5rem_5.5rem_minmax(10rem,1fr)] items-center gap-x-3";

  return (
    <section className="flex flex-col gap-4">
      <h3
        className={`text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Throughput
      </h3>
      <div
        className={`rounded-lg border overflow-hidden ${c("border-ink-border", "border-light-border")}`}
      >
        <div
          className={`${gridCols} px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c("text-text-muted border-ink-border-subtle bg-ink-well", "text-light-text-muted border-light-border-subtle bg-light-well")}`}
        >
          <span>Stage</span>
          <span>Node</span>
          <span />
          <span className="text-right">Records</span>
          <span className="text-right">Data</span>
          <span>Status</span>
        </div>
        <StageRows
          label="Append"
          title="Origin ingress: records/s appended to this vault's working segments, per writing node"
          rows={append}
          gridCols={gridCols}
          dark={dark}
          idleNote={appendIdleNote}
        />
        <StageRows
          label="Collected"
          title="Home ingress: records/s arriving in head/ per home node. Every placement member collects its own copy, so the sum counts each record once PER HOME — with RF=4, one appended record is collected up to four times. This measures replication work, not record throughput."
          rows={collected}
          gridCols={gridCols}
          dark={dark}
          replicated
          idleNote={collectIdleNote}
          totalsNote={replicationNote(collected)}
        />
        <StageRows
          label="Sealed"
          title="Records/s materialized into sealed GLCB chunks per home node. Every home builds its own GLCB, so the sum counts each record once PER HOME — replication work, not record throughput."
          rows={sealed}
          gridCols={gridCols}
          dark={dark}
          replicated
          idleNote={sealIdleNote}
          totalsNote={replicationNote(sealed)}
        />
      </div>
    </section>
  );
}

// stageRowActive: nonzero rate, standing queue, or recent spark history.
// Inactive rows still render — their STATUS column says why they're quiet.
function stageRowActive(r: StageRow): boolean {
  return (
    r.recordsPerSec > 0 ||
    r.bytesPerSec > 0 ||
    (r.extra?.depth ?? 0) > 0 ||
    r.spark.some((v) => v > 0)
  );
}

function StageRows({
  label,
  title,
  rows,
  gridCols,
  dark,
  replicated,
  idleNote,
  totalsNote,
}: Readonly<{
  label: string;
  title: string;
  rows: StageRow[];
  gridCols: string;
  dark: boolean;
  replicated?: boolean;
  idleNote?: (r: StageRow) => IdleNote;
  totalsNote?: IdleNote;
}>) {
  const c = useThemeClass(dark);
  const sorted = rows.toSorted((a, b) => a.node.localeCompare(b.node));
  const totalRecords = sorted.reduce((sum, r) => sum + r.recordsPerSec, 0);
  const totalBytes = sorted.reduce((sum, r) => sum + r.bytesPerSec, 0);
  const stageClass = `text-[0.75em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted", "text-light-text-muted")}`;
  const brightMono = `font-mono text-right ${c("text-text-bright", "text-light-text-bright")}`;
  const mutedMono = `font-mono text-right ${c("text-text-muted", "text-light-text-muted")}`;
  const rowBorder = c("border-ink-border-subtle", "border-light-border-subtle");
  const rowClass = `${gridCols} px-4 py-1.5 text-[0.85em] border-b last:border-b-0 ${rowBorder}`;

  return (
    <>
      {sorted.length > 1 && (
        <div className={rowClass} title={title}>
          <span className={stageClass}>{label}</span>
          <span className={`font-mono whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}>
            {replicated ? `Σ ${sorted.length} homes` : "all nodes"}
          </span>
          <span />
          <span className={brightMono}>{formatRate(totalRecords)}/s</span>
          <span className={brightMono}>{formatBytes(totalBytes)}/s</span>
          <span
            className={`font-mono whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
            title="Replicated stages count each record once per home: Σ ≈ append × homes when replication keeps pace; 'catch-up' when a rejoined node backfills or backlog drains."
          >
            {totalsNote?.text ?? ""}
          </span>
        </div>
      )}
      {sorted.map((r, i) => {
        const isActive = stageRowActive(r);
        const note = isActive ? undefined : (idleNote?.(r) ?? { text: "idle", warn: false });
        const rateClass = sorted.length > 1 || !isActive ? mutedMono : brightMono;
        return (
          <div key={r.node} className={rowClass} title={title}>
            <span className={stageClass}>{sorted.length === 1 && i === 0 ? label : ""}</span>
            <span className={`font-mono truncate ${c("text-text-muted", "text-light-text-muted")}`} title={r.node}>
              {r.node}
            </span>
            <span className="text-copper">
              <Spark values={r.spark} />
            </span>
            <span className={rateClass}>{formatRate(r.recordsPerSec)}/s</span>
            <span className={rateClass}>{formatBytes(r.bytesPerSec)}/s</span>
            <span className="flex items-center gap-2 whitespace-nowrap">
              {note && (
                <span
                  className={`font-mono ${note.warn ? "text-severity-warn" : c("text-text-muted", "text-light-text-muted")}`}
                  title="Why this node is quiet on this stage. 'Caught up' / 'up to date' is healthy; 'behind' while idle means this node's stage has stalled."
                >
                  {note.text}
                </span>
              )}
              {isActive && r.extra && r.extra.depth > 0 && (
                <span
                  className="font-mono text-severity-warn"
                  title="Segmentation queue depth / capacity"
                >
                  queue {r.extra.depth}/{r.extra.cap}
                </span>
              )}
              {isActive && r.extra && r.extra.durablePerSec + 1 < r.recordsPerSec && (
                <span
                  className="font-mono text-severity-warn"
                  title="Durable-commit rate lags the append rate — fsync backpressure"
                >
                  durable {formatRate(r.extra.durablePerSec)}/s
                </span>
              )}
            </span>
          </div>
        );
      })}
    </>
  );
}

function ValidateVaultButton({
  vaultId,
  dark,
}: Readonly<{
  vaultId: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const validate = useValidateVault();
  const { addToast } = useToast();

  return (
    <button
      type="button"
      className={`px-2.5 py-1 text-[0.8em] rounded border transition-colors ${c(
        "border-ink-border-subtle text-text-muted hover:bg-ink-hover",
        "border-light-border-subtle text-light-text-muted hover:bg-light-hover",
      )}`}
      disabled={validate.isPending}
      onClick={async () => {
        try {
          const result = await validate.mutateAsync(vaultId);
          if (result.valid) {
            addToast(
              `Vault valid (${result.chunks.length} chunk(s) checked)`,
              "info",
            );
          } else {
            const issues = result.chunks
              .filter((ch) => !ch.valid)
              .map((ch) => `${encode(ch.chunkId)}: ${ch.issues.join(", ")}`)
              .join("; ");
            addToast(`Validation failed: ${issues}`, "error");
          }
        } catch (err: unknown) {
          addToast(err instanceof Error ? err.message : "Validation failed", "error");
        }
      }}
    >
      {validate.isPending ? "Validating..." : "Validate"}
    </button>
  );
}

function followerDisplayName(
  nodeId: string,
  storageClass: number,
  nscs: readonly { nodeId: Uint8Array; fileStorages: { storageClass: number }[] }[],
  nodeNameMap: Map<string, string>,
): ReactNode {
  const name = resolveNodeName(nodeNameMap, nodeId);
  if (storageClass <= 0) return name;
  const nsc = nscs.find((n) => encode(n.nodeId) === nodeId);
  const hasExact = nsc?.fileStorages.some((a) => a.storageClass === storageClass);
  if (hasExact || !nsc || nsc.fileStorages.length === 0) return name;
  const fallbackClass = nsc.fileStorages[0]!.storageClass;
  return (
    <>
      {name}
      <span className="text-severity-warn">{` (class ${String(fallbackClass)})`}</span>
    </>
  );
}

function VaultLeaderSummary({
  vaultId,
  vaultTypeLabel,
  dark,
}: Readonly<{ vaultId: string; vaultTypeLabel: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();

  const sectionTitle = (
    <h3
      className={`text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
    >
      Topology
    </h3>
  );

  const panelClass = `rounded-lg border px-4 py-3 ${c("border-ink-border bg-ink-well", "border-light-border bg-light-well")}`;

  if (isLoading || !config) {
    return (
      <section className="flex flex-col gap-4">
        {sectionTitle}
        <div className={panelClass}>
          <LoadingPlaceholder dark={dark} />
        </div>
      </section>
    );
  }

  const nscs = config.nodeStorageConfigs ?? [];
  const nodeNameMap = buildNodeNameMap(config.nodeConfigs ?? []);
  const vaultCfg = config.vaults.find((v) => encode(v.id) === vaultId);

  const rf = vaultCfg?.replicationFactor || 1;
  const leaderId = vaultCfg ? leaderNodeId(vaultCfg, nscs) : "";
  const leaderName = leaderId ? resolveNodeName(nodeNameMap, leaderId) : "";
  const followerIds = vaultCfg ? followerNodeIds(vaultCfg, nscs) : [];

  return (
    <section className="flex flex-col gap-4">
      {sectionTitle}
      <VaultPlacementSummary
        leaderName={leaderName}
        followerIds={followerIds}
        rf={rf}
        storageClass={vaultCfg?.storageClass ?? 0}
        jsonlPath={vaultTypeLabel === "jsonl" ? vaultCfg?.path : undefined}
        nscs={nscs}
        nodeNameMap={nodeNameMap}
        dark={dark}
      />
    </section>
  );
}

function VaultPlacementSummary({
  leaderName,
  followerIds,
  rf,
  storageClass,
  jsonlPath,
  nscs,
  nodeNameMap,
  dark,
}: Readonly<{
  leaderName: string;
  followerIds: string[];
  rf: number;
  storageClass: number;
  jsonlPath?: string;
  nscs: readonly { nodeId: Uint8Array; fileStorages: { storageClass: number }[] }[];
  nodeNameMap: Map<string, string>;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const labelClass = `text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted", "text-light-text-muted")}`;
  const valueClass = c("text-text-bright", "text-light-text-bright");

  return (
    <div
      className={`rounded-lg border px-4 py-3 ${c("border-ink-border bg-ink-well", "border-light-border bg-light-well")}`}
    >
      <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-[0.85em]">
        <div className="flex items-baseline gap-2">
          <span className={labelClass}>Leader</span>
          <span className={`font-mono ${leaderName ? valueClass : c("text-text-muted", "text-light-text-muted")}`}>
            {leaderName || "unplaced"}
          </span>
        </div>
        {rf > 1 && <Badge variant="info" dark={dark}>{`RF=${String(rf)}`}</Badge>}
        {jsonlPath && (
          <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>{jsonlPath}</span>
        )}
      </div>
      {followerIds.length > 0 && (
        <div className="mt-2 flex flex-wrap items-baseline gap-x-2 gap-y-1 text-[0.85em]">
          <span className={labelClass}>Followers</span>
          <span className={valueClass}>
            {followerIds.map((id, si) => (
              <span key={id}>
                {si > 0 && ", "}
                <span className="font-mono">
                  {followerDisplayName(id, storageClass, nscs, nodeNameMap)}
                </span>
              </span>
            ))}
          </span>
        </div>
      )}
    </div>
  );
}

function ChunkList({ vaultId, dark }: Readonly<{ vaultId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: chunks, isLoading } = useChunks(vaultId);
  const { data: config } = useConfig();
  const registry = useNodeRegistry();
  const [expandedChunk, setExpandedChunk] = useState<string | null>(null);
  // Reachable node NAMES for the missing-pip state (dashed red ring).
  const liveNodes = new Set<string>();
  for (const n of registry.all) {
    if (n.isLive) liveNodes.add(n.name);
  }

  const vaultMatches = (config?.vaults ?? []).filter((v) => encode(v.id) === vaultId);

  if (isLoading) {
    return (
      <div
        className={`px-4 py-3 text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Loading chunks...
      </div>
    );
  }

  // Backend already deduplicates chunks and populates replica_count. Each
  // chunk appears exactly once with authoritative metadata from the leader.
  const dedupedChunks = chunks ?? [];

  // Group chunks by vault, then sort within each group by time (newest first).
  const chunkGroups = new Map<string, { vaultType: string; chunks: ChunkMeta[] }>();
  for (const chunk of dedupedChunks) {
    const key = encode(chunk.vaultId) || "unknown";
    const existing = chunkGroups.get(key);
    if (existing) {
      existing.chunks.push(chunk);
    } else {
      chunkGroups.set(key, { vaultType: chunk.vaultType, chunks: [chunk] });
    }
  }

  // Node name resolution — used by both local and remote vault headers.
  const nodeNameMap = buildNodeNameMap(config?.nodeConfigs ?? []);

  const sortChunks = (arr: ChunkMeta[]) =>
    arr.toSorted((a, b) => {
      const aTs = a.ingestStart ?? a.writeStart;
      const bTs = b.ingestStart ?? b.writeStart;
      const aTime = aTs ? instantToMs(protoToInstant(aTs)) : 0;
      const bTime = bTs ? instantToMs(protoToInstant(bTs)) : 0;
      return bTime - aTime;
    });

  // One placement summary per vault, then a single chunk table. Placement used
  // to render as a colspan row between the table header and chunk rows (gastrolog-28yi3).
  const nscs = config?.nodeStorageConfigs ?? [];
  const vaultIds = vaultMatches.map((v: { id: Uint8Array }) => encode(v.id));
  const chunkRows = vaultIds.flatMap((vId: string) => {
    const group = chunkGroups.get(vId);
    if (!group) return [];
    const vaultCfg = config?.vaults.find((v) => encode(v.id) === vId);
    const rf = vaultCfg?.replicationFactor || 1;
    const secondaries = vaultCfg ? followerNodeIds(vaultCfg, nscs) : [];
    const pnId = vaultCfg ? leaderNodeId(vaultCfg, nscs) : "";
    // Blob-mark label: the operator-configured cloud store name for this
    // vault. Vendor words appear only as operator-supplied labels.
    const cloudSvcId = vaultCfg ? encode(vaultCfg.cloudServiceId) : "";
    const storeLabel =
      (cloudSvcId && config?.cloudServices.find((svc) => encode(svc.id) === cloudSvcId)?.name) ||
      "cloud";
    return sortChunks(group.chunks).map((chunk) => {
      const start = chunkStartInstant(chunk);
      const end = chunkEndInstant(chunk, start);
      const isExpanded = expandedChunk === encode(chunk.id);
      const residentNodes = chunk.replicaNodeIds.map((id) =>
        resolveNodeName(nodeNameMap, id),
      );
      const placementNodes = vaultCfg
        ? [pnId, ...secondaries].filter(Boolean).map((id) => resolveNodeName(nodeNameMap, id))
        : [];
      const pendingAckNodes = chunk.pendingAckNodeIds.map((id) =>
        resolveNodeName(nodeNameMap, id),
      );
      return (
        <ChunkRow
          key={encode(chunk.id)}
          chunk={chunk}
          vaultId={vaultId}
          start={start}
          end={end}
          isExpanded={isExpanded}
          onToggle={() => setExpandedChunk(isExpanded ? null : encode(chunk.id))}
          dark={dark}
          c={c}
          rf={rf}
          residentNodes={residentNodes}
          placementNodes={placementNodes}
          pendingAckNodes={pendingAckNodes}
          liveNodes={liveNodes}
          storeLabel={storeLabel}
        />
      );
    });
  });

  return (
    <div>
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3
          className={`text-[0.75em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Chunks
        </h3>
        <ValidateVaultButton vaultId={vaultId} dark={dark} />
      </div>

      {chunkRows.length > 0 ? (
        <div
          className={`rounded-lg border overflow-hidden ${c("border-ink-border", "border-light-border")}`}
        >
          <table className="w-full border-collapse">
            <thead>
              <tr
                className={`text-left text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
                  "text-text-muted border-ink-border-subtle bg-ink-well",
                  "text-light-text-muted border-light-border-subtle bg-light-well",
                )}`}
              >
                <th className="px-4 py-2 font-medium">Chunk ID</th>
                <th className="px-2 py-2 font-medium">Time Range</th>
                <th className="px-2 py-2 font-medium">Status</th>
                <th className="px-2 py-2 font-medium text-right">Records</th>
                <th className="px-4 py-2 font-medium text-right">Size</th>
              </tr>
            </thead>
            <tbody>{chunkRows}</tbody>
          </table>
        </div>
      ) : (
        <div className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
          No chunks on this node yet.
        </div>
      )}
    </div>
  );
}

// ChunkReplicaPips is the expanded-pane variant of the seal-pip row: the
// same grammar at larger size, with each node's name beside its pip so the
// operator can read the row without hovering. Supersedes the old per-node
// Badge list — one visual language for copy state everywhere.
function ChunkReplicaPips({
  chunk,
  placementNodes,
  residentNodes,
  pendingAckNodes,
  liveNodes,
  dark,
}: Readonly<{
  chunk: ChunkMeta;
  placementNodes: string[];
  residentNodes: string[];
  pendingAckNodes: string[];
  liveNodes: ReadonlySet<string>;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const { pips, ghosts } = computePips({
    chunkState: chunkLifecycleState(chunk),
    placementNodes,
    residentNodes,
    pendingAckNodes,
    deleteInFlight: pendingAckNodes.length > 0,
    liveNodes,
  });
  const all = [...pips, ...ghosts.map((g) => ({ ...g, isGhost: true }))];
  if (all.length === 0) return null;
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5">
      {all.map((p) => (
        <span key={p.node} className="inline-flex items-center gap-1.5" title={p.title}>
          <SealPips pips={[p]} ghosts={[]} size={14} />
          <span className={`font-mono text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
            {p.node}
          </span>
        </span>
      ))}
    </div>
  );
}

function ChunkRow({
  chunk,
  vaultId,
  start,
  end,
  isExpanded,
  onToggle,
  dark,
  c,
  rf,
  residentNodes,
  placementNodes,
  pendingAckNodes,
  liveNodes,
  storeLabel,
}: Readonly<{
  chunk: ChunkMeta;
  vaultId: string;
  start: Date | undefined;
  end: Date | undefined;
  isExpanded: boolean;
  onToggle: () => void;
  dark: boolean;
  c: (darkCls: string, lightCls: string) => string;
  rf: number;
  residentNodes: string[];
  placementNodes: string[];
  pendingAckNodes: string[];
  liveNodes: ReadonlySet<string>;
  storeLabel: string;
}>) {
  return (
    <>
      <tr
        className={`border-b text-[0.85em] cursor-pointer transition-colors ${c(
          "border-ink-border-subtle hover:bg-ink-hover",
          "border-light-border-subtle hover:bg-light-hover",
        )} ${isExpanded ? c("bg-ink-hover", "bg-light-hover") : ""}`}
        onClick={onToggle}
        {...clickableProps(onToggle)}
        aria-expanded={isExpanded}
      >
        <td className="px-4 py-2 whitespace-nowrap">
          <span
            className={`text-[0.6em] transition-transform inline-block mr-1.5 ${isExpanded ? "rotate-90" : ""} ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {"\u25B6"}
          </span>
          <span
            className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
            title={encode(chunk.id)}
          >
            {middleTruncate(encode(chunk.id), 16, 8, 5)}
          </span>
        </td>
        <td className="px-2 py-2">
          <span
            className={`text-[0.95em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {start ? formatDateTimeShort(start) : "\u2014"}
            <span className={`mx-1.5 ${c("text-text-muted", "text-light-text-muted")}`}>
              {"\u2192"}
            </span>
            {end ? formatDateTimeShort(end) : "\u2014"}
          </span>
        </td>
        <td className="px-2 py-2">
          <span className="flex items-center gap-1.5 whitespace-nowrap">
            {chunk.cloudBacked ? (
              <BlobMark label={storeLabel} uploading={false} />
            ) : (
              (() => {
                const { pips, ghosts } = computePips({
                  chunkState: chunkLifecycleState(chunk),
                  placementNodes,
                  residentNodes,
                  pendingAckNodes,
                  deleteInFlight: pendingAckNodes.length > 0,
                  liveNodes,
                });
                return <SealPips pips={pips} ghosts={ghosts} />;
              })()
            )}
            {chunk.compressed && (
              <Badge variant="info" dark={dark}>compr</Badge>
            )}
            {chunk.archived && (
              <Badge variant="warn" dark={dark}>{chunk.storageClass || "archived"}</Badge>
            )}
            {chunk.retentionPending && (
              <Badge
                variant="warn"
                dark={dark}
                title="Retention pending — chunk is queued for retention firing"
              >
                ret
              </Badge>
            )}
          </span>
        </td>
        <td className={`px-2 py-2 text-right font-mono whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}>
          {Number(chunk.recordCount).toLocaleString()}
        </td>
        <td
          className={`px-4 py-2 text-right font-mono whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
          title={chunk.compressed && Number(chunk.diskBytes) > 0
            ? `${formatBytes(Number(chunk.bytes))} \u2192 ${formatBytes(Number(chunk.diskBytes))} on disk`
            : undefined}
        >
          {chunk.compressed && Number(chunk.diskBytes) > 0
            ? formatBytes(Number(chunk.diskBytes))
            : formatBytes(Number(chunk.bytes))}
        </td>
      </tr>
      {isExpanded && (
        <tr>
          <td colSpan={5} className="p-0">
            <ChunkDetail
              vaultId={vaultId}
              chunk={chunk}
              dark={dark}
              rf={rf}
              residentNodes={residentNodes}
              placementNodes={placementNodes}
              pendingAckNodes={pendingAckNodes}
              liveNodes={liveNodes}
            />
          </td>
        </tr>
      )}
    </>
  );
}

function ChunkDetail({
  vaultId,
  chunk,
  dark,
  rf,
  residentNodes,
  placementNodes,
  pendingAckNodes,
  liveNodes,
}: Readonly<{
  vaultId: string;
  chunk: ChunkMeta;
  dark: boolean;
  rf: number;
  residentNodes: string[];
  placementNodes: string[];
  pendingAckNodes: string[];
  liveNodes: ReadonlySet<string>;
}>) {
  const c = useThemeClass(dark);
  // Skip index fetch for cloud-backed chunks — they don't have local indexes.
  const { data, isLoading } = useIndexes(vaultId, chunk.cloudBacked ? "" : encode(chunk.id));

  const logicalBytes = Number(chunk.bytes);
  const diskBytes = Number(chunk.diskBytes);
  const showCompression = chunk.compressed && diskBytes > 0 && logicalBytes > 0;
  const reductionPct = showCompression
    ? Math.round((1 - diskBytes / logicalBytes) * 100)
    : 0;

  return (
    <div className={`px-4 py-3 ${c("bg-ink-raised", "bg-light-bg")}`}>
      {/* Full chunk ID — selectable for copy/paste */}
      <div className="mb-3">
        <div
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Chunk ID
        </div>
        <div
          className={`font-mono text-[0.85em] select-all ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {encode(chunk.id)}
        </div>
      </div>

      {/* Replication info — only shown when RF > 1 */}
      {rf > 1 && (
        <div className="mb-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Replicas
          </div>
          <ChunkReplicaPips
            chunk={chunk}
            placementNodes={placementNodes}
            residentNodes={residentNodes}
            pendingAckNodes={pendingAckNodes}
            liveNodes={liveNodes}
            dark={dark}
          />
          {placementNodes.length < rf && (
            <div className="mt-1 text-[0.8em] text-severity-error">
              Not enough nodes with the required storage class to satisfy RF={String(rf)}
            </div>
          )}
        </div>
      )}

      {/* Compression / storage info */}
      {showCompression && (
        <div className="mb-3">
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Compression
          </div>
          <div className={`flex items-center gap-3 text-[0.85em]`}>
            <span
              className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {formatBytes(logicalBytes)} &rarr; {formatBytes(diskBytes)}
            </span>
            <span
              className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {reductionPct}% reduction
            </span>
          </div>
        </div>
      )}

      {/* Active chunks: no indexes yet */}
      {!chunk.sealed && (
        <div
          className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Indexes are built when the chunk is sealed.
        </div>
      )}
      {chunk.sealed && chunk.cloudBacked && (
        <>
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Cloud Storage
          </div>
          <div className="flex flex-col gap-1.5">
            <div className={`flex items-center gap-3 text-[0.85em]`}>
              <span className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}>blob</span>
              <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                {formatBytes(Number(chunk.diskBytes))}
              </span>
              <span className={c("text-text-muted", "text-light-text-muted")}>
                GLCB{chunk.cloudBacked ? " (zstd-wrapped on transport)" : ""}
              </span>
            </div>
            <div className={`flex items-center gap-3 text-[0.85em]`}>
              <span className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}>ts-index</span>
              <Badge variant="info" dark={dark}>embedded</Badge>
              <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                {Number(chunk.recordCount).toLocaleString()} entries
              </span>
            </div>
            <div className={`flex items-center gap-3 text-[0.85em]`}>
              <span className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}>class</span>
              <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                {chunk.storageClass || "standard"}
              </span>
              {!chunk.archived && (
                <ArchiveButton vaultId={vaultId} chunkId={encode(chunk.id)} dark={dark} />
              )}
              {chunk.archived && (
                <RestoreButton vaultId={vaultId} chunkId={encode(chunk.id)} dark={dark} />
              )}
            </div>
          </div>
        </>
      )}
      {chunk.sealed && !chunk.cloudBacked && (
        <>
          {/* Local indexes */}
          <div
            className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Indexes
          </div>

          {isLoading && (
            <div
              className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Loading indexes...
            </div>
          )}
          {!isLoading && (!data?.indexes || data.indexes.length === 0) && (
            <div
              className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
            >
              No indexes.
            </div>
          )}
          {!isLoading && data?.indexes && data.indexes.length > 0 && (
            <div className="flex flex-col gap-1.5">
              {data.indexes.map((idx) => (
                <div
                  key={idx.name}
                  className={`flex items-center gap-3 text-[0.85em]`}
                >
                  <span
                    className={`font-mono w-20 ${c("text-text-bright", "text-light-text-bright")}`}
                  >
                    {idx.name}
                  </span>
                  {idx.exists ? (
                    <>
                      <Badge variant="info" dark={dark}>ok</Badge>
                      <span
                        className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                      >
                        {Number(idx.entryCount).toLocaleString()} entries
                      </span>
                      <span
                        className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                      >
                        {formatBytes(Number(idx.sizeBytes))}
                      </span>
                    </>
                  ) : (
                    <Badge variant="muted" dark={dark}>missing</Badge>
                  )}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function ArchiveButton({ vaultId, chunkId, dark }: Readonly<{ vaultId: string; chunkId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const archive = useArchiveChunk();
  const { addToast } = useToast();
  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        archive.mutate(
          { vaultId, chunkId },
          {
            onSuccess: () => addToast("Chunk archived to Glacier", "info"),
            onError: (err) => addToast(err instanceof Error ? err.message : "Archive failed", "error"),
          },
        );
      }}
      disabled={archive.isPending}
      title="Archive chunk to offline storage"
      className={`px-2 py-0.5 text-[0.8em] rounded border transition-colors ${c(
        "border-ink-border text-text-muted hover:text-copper hover:border-copper/40 hover:bg-ink-hover",
        "border-light-border text-light-text-muted hover:text-copper hover:border-copper/40 hover:bg-light-hover",
      )}`}
    >
      {archive.isPending ? "Archiving..." : "Archive"}
    </button>
  );
}

function RestoreButton({ vaultId, chunkId, dark }: Readonly<{ vaultId: string; chunkId: string; dark: boolean }>) {
  const c = useThemeClass(dark);
  const restore = useRestoreChunk();
  const { addToast } = useToast();
  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        restore.mutate(
          { vaultId, chunkId },
          {
            onSuccess: () => addToast("Chunk restore initiated", "info"),
            onError: (err) => addToast(err instanceof Error ? err.message : "Restore failed", "error"),
          },
        );
      }}
      disabled={restore.isPending}
      title="Restore chunk from offline storage"
      className={`px-2 py-0.5 text-[0.8em] rounded border transition-colors ${c(
        "border-ink-border text-severity-warn hover:text-copper hover:border-copper/40 hover:bg-ink-hover",
        "border-light-border text-severity-warn hover:text-copper hover:border-copper/40 hover:bg-light-hover",
      )}`}
    >
      {restore.isPending ? "Restoring..." : "Restore"}
    </button>
  );
}

