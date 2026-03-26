import { useState } from "react";
import { protoInt64 } from "@bufbuild/protobuf";
import type { VaultConfig, RouteConfig, NodeConfig } from "../../api/gen/gastrolog/v1/config_pb";
import type { NodeStorageConfig } from "../../api/gen/gastrolog/v1/storage_pb";
import { TierConfig, TierType, RetentionRule } from "../../api/gen/gastrolog/v1/config_pb";
import {
  usePutVault,
  useDeleteVault,
  useSealVault,
  useReindexVault,
  useMigrateVault,
  useMergeVaults,
  usePutTier,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, SelectInput, NumberInput } from "./FormField";
import { Button, DropdownButton } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { PulseIcon } from "../icons";
import { CrossLinkBadge } from "../inspector/CrossLinkBadge";
import { JobProgress } from "./VaultHelpers";
import { MigrateVaultForm, MergeVaultForm } from "./VaultMigrateForms";
import { useThemeClass } from "../../hooks/useThemeClass";
function formatBytes(b: bigint | number): string {
  const n = typeof b === "bigint" ? Number(b) : b;
  if (n >= 1024 ** 4) return `${(n / 1024 ** 4).toFixed(1)} TB`;
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(1)} GB`;
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  if (n >= 1024) return `${(n / 1024).toFixed(0)} KB`;
  return `${String(n)} B`;
}

function tierTypeLabel(type: TierType): string {
  switch (type) {
    case TierType.MEMORY: return "memory";
    case TierType.FILE: return "file";
    case TierType.CLOUD: return "cloud";
    default: return "unknown";
  }
}

import {
  TierEntryCard,
  emptyTierEntry,
  tierTypeEnum,
  parseMemoryBudget,
  isTierComplete,
  type TierEntry,
  type TierTypeLabel,
  retentionActionForPosition,
} from "./VaultsSettings";

interface VaultSettingsCardProps {
  vault: VaultConfig;
  vaults: VaultConfig[];
  tiers: TierConfig[];
  routes: RouteConfig[];
  nodeConfigs: NodeConfig[];
  nodeStorageConfigs: NodeStorageConfig[];
  storageClassOptions: { value: string; label: string }[];
  cloudServiceOptions: { value: string; label: string }[];
  rotationPolicyOptions: { value: string; label: string }[];
  retentionPolicyOptions: { value: string; label: string }[];
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onOpenInspector?: (inspectorParam: string) => void;
}

export function VaultSettingsCard({
  vault,
  vaults,
  tiers,
  routes: _routes,
  nodeConfigs,
  nodeStorageConfigs,
  storageClassOptions,
  cloudServiceOptions,
  rotationPolicyOptions,
  retentionPolicyOptions,
  dark,
  expanded,
  onToggle,
  onOpenInspector,
}: Readonly<VaultSettingsCardProps>) {
  const c = useThemeClass(dark);
  const putVault = usePutVault();
  const putTier = usePutTier();
  const deleteVault = useDeleteVault();
  const seal = useSealVault();
  const reindex = useReindexVault();
  const migrate = useMigrateVault();
  const merge = useMergeVaults();
  const { addToast } = useToast();

  // Inline tier creation state.
  const [newTier, setNewTier] = useState<TierEntry | null>(null);
  // Per-vault state — previously Record maps in the parent.
  const [deleteData, setDeleteData] = useState(false);
  const [migrateTarget, setMigrateTarget] = useState<{ name: string; type: string; dir: string } | null>(null);
  const [mergeTarget, setMergeTarget] = useState<string | null>(null);
  const [activeJob, setActiveJob] = useState<{ jobId: string; label: string } | null>(null);

  // All vault+tier edits in one place. Initialized from props, reset on cancel/save.
  interface VaultEdit {
    name: string;
    enabled: boolean;
    tierIds: string[];
    tierRotation: Record<string, string>;    // tierId → rotationPolicyId
    tierRetention: Record<string, string>;   // tierId → retentionPolicyId
    tierRF: Record<string, string>;          // tierId → replicationFactor (string during editing)
    tierStorageClass: Record<string, string>; // tierId → storageClass (string during editing)
  }

  const buildInitialEdit = (): VaultEdit => ({
    name: vault.name,
    enabled: vault.enabled,
    tierIds: [...vault.tierIds],
    tierRotation: Object.fromEntries(
      tiers.filter((t) => vault.tierIds.includes(t.id)).map((t) => [t.id, t.rotationPolicyId]),
    ),
    tierRetention: Object.fromEntries(
      tiers.filter((t) => vault.tierIds.includes(t.id)).map((t) => [t.id, t.retentionRules[0]?.retentionPolicyId ?? ""]),
    ),
    tierRF: Object.fromEntries(
      tiers.filter((t) => vault.tierIds.includes(t.id)).map((t) => [t.id, String(t.replicationFactor || 1)]),
    ),
    tierStorageClass: Object.fromEntries(
      tiers.filter((t) => vault.tierIds.includes(t.id)).map((t) => [t.id, String(t.storageClass || 0)]),
    ),
  });

  const [edit, setEditState] = useState<VaultEdit>(buildInitialEdit);
  const resetEdit = () => setEditState(buildInitialEdit());

  // Sync edit state with props after save. When `pendingReset` is true,
  // the next props change unconditionally resets the edit state.
  const initial = buildInitialEdit();
  const initialJson = JSON.stringify(initial);
  const [pendingReset, setPendingReset] = useState(false);
  const [lastInitialJson, setLastInitialJson] = useState(initialJson);
  if (initialJson !== lastInitialJson) {
    setLastInitialJson(initialJson);
    if (pendingReset) {
      setEditState(initial);
      setPendingReset(false);
    }
  }

  const setEdit = (patch: Partial<VaultEdit>) => setEditState((prev) => ({ ...prev, ...patch }));
  const setTierRotationPolicy = (tierId: string, value: string) =>
    setEditState((prev) => ({ ...prev, tierRotation: { ...prev.tierRotation, [tierId]: value } }));
  const setTierRetentionPolicyId = (tierId: string, value: string) =>
    setEditState((prev) => ({ ...prev, tierRetention: { ...prev.tierRetention, [tierId]: value } }));
  const setTierRF = (tierId: string, value: string) =>
    setEditState((prev) => ({ ...prev, tierRF: { ...prev.tierRF, [tierId]: value } }));
  const setTierStorageClass = (tierId: string, value: string) =>
    setEditState((prev) => ({ ...prev, tierStorageClass: { ...prev.tierStorageClass, [tierId]: value } }));

  const anyDirty = JSON.stringify(edit) !== initialJson || newTier !== null;

  // Aliases for backward compat with the rest of the component.
  const localTierIds = edit.tierIds;
  const setLocalTierIds = (ids: string[]) => setEdit({ tierIds: ids });
  const getTierRotationPolicy = (tierId: string): string => edit.tierRotation[tierId] ?? "";
  const getTierRetentionPolicyId = (tierId: string): string => edit.tierRetention[tierId] ?? "";

  // Resolve vault's tiers from local state (reflects unsaved reorder/remove).
  const tierMap = new Map(tiers.map((t) => [t.id, t]));
  const vaultTiers = localTierIds.map((id) => tierMap.get(id)).filter((t): t is TierConfig => !!t);

  // Node name resolution for tier placement display.
  const nodeNameMap = new Map(nodeConfigs.map((n) => [n.id, n.name || n.id]));
  const resolveNodeName = (nodeId: string) => nodeNameMap.get(nodeId) || nodeId;

  // Check if a node has a specific storage class; returns the fallback class if not.
  const nodeStorageClass = (nodeId: string, requiredClass: number): { exact: boolean; actualClass: number } => {
    const nsc = nodeStorageConfigs.find((n) => n.nodeId === nodeId);
    if (!nsc) return { exact: false, actualClass: 0 };
    if (nsc.areas.some((a) => a.storageClass === requiredClass)) return { exact: true, actualClass: requiredClass };
    const first = nsc.areas[0];
    return { exact: false, actualClass: first?.storageClass ?? 0 };
  };

  const { handleSave: saveVault, handleDelete } = useCrudHandlers({
    mutation: putVault,
    deleteMutation: deleteVault,
    label: "Vault",
    onSaveTransform: (
      id,
      e: {
        name: string;
        tierIds: string[];
        enabled: boolean;
      },
    ) => ({
      id,
      name: e.name,
      tierIds: e.tierIds,
      enabled: e.enabled,
    }),
    onDeleteTransform: (id) => ({ id, force: true, deleteData }),
    // Don't reset edit state eagerly — props are stale inside the async
    // handler. The edit state stays as-is; anyDirty becomes false naturally
    // when the parent re-renders with updated config from setQueryData.
  });

  const warnings: string[] = [];
  if (vaultTiers.length === 0) warnings.push("no tiers configured");

  return (
    <SettingsCard
      key={vault.id}
      id={vault.name || vault.id}
      typeBadge={vaultTiers.length > 0 ? vaultTiers.map((t) => tierTypeLabel(t.type)).join(", ") : undefined}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={() => handleDelete(vault.id)}
      deleteLabel="Delete"
      deleteConfirmExtra={vaultTiers.some((t) => t.type === TierType.FILE) ? (
        <label className="flex items-center gap-1.5 text-[0.8em] opacity-70">
          <input
            type="checkbox"
            checked={deleteData}
            onChange={(e) => setDeleteData(e.target.checked)}
          />
          Delete data files
        </label>
      ) : undefined}
      footer={
        <>
          {activeJob && (
            <JobProgress
              jobId={activeJob.jobId}
              label={activeJob.label}
              dark={dark}
              onComplete={(job) => {
                const chunks = Number(job.chunksDone);
                const errors = job.errorDetails.length;
                const errorSuffix = errors > 0 ? ", " + String(errors) + " error(s)" : "";
                addToast(
                  activeJob.label + " done: " + String(chunks) + " chunk(s)" + errorSuffix,
                  errors > 0 ? "warn" : "info",
                );
                setActiveJob(null);
              }}
              onFailed={(job) => {
                addToast(`${activeJob.label} failed: ${job.error}`, "error");
                setActiveJob(null);
              }}
            />
          )}
          <Button
            variant="ghost"
            bordered
            dark={dark}
            disabled={seal.isPending || !!activeJob}
            onClick={async () => {
              try {
                await seal.mutateAsync(vault.id);
                addToast("Active chunk rotated", "info");
              } catch (err: unknown) {
                addToast(err instanceof Error ? err.message : "Rotate failed", "error");
              }
            }}
          >
            {seal.isPending ? "Rotating..." : "Rotate"}
          </Button>
          <Button
            variant="ghost"
            bordered
            dark={dark}
            disabled={reindex.isPending || !!activeJob}
            onClick={async () => {
              try {
                const result = await reindex.mutateAsync(vault.id);
                setActiveJob({ jobId: result.jobId, label: "Reindexing" });
              } catch (err: unknown) {
                addToast(err instanceof Error ? err.message : "Reindex failed", "error");
              }
            }}
          >
            {activeJob?.label === "Reindexing" ? "Reindexing..." : "Reindex"}
          </Button>
          {vault.enabled && (
            <Button
              variant="ghost"
              bordered
              dark={dark}
              disabled={!!activeJob}
              onClick={() => setMigrateTarget((prev) => prev ? null : { name: "", type: "", dir: "" })}
            >
              {migrateTarget ? "Cancel Migrate" : "Migrate"}
            </Button>
          )}
          <Button
            variant="ghost"
            bordered
            dark={dark}
            disabled={!!activeJob}
            onClick={() => setMergeTarget((prev) => prev !== null ? null : "")}
          >
            {mergeTarget !== null ? "Cancel Merge" : "Merge Into..."}
          </Button>
          {(anyDirty) && (
            <Button
              variant="ghost"
              bordered
              dark={dark}
              onClick={() => { resetEdit(); setNewTier(null); }}
            >
              Cancel
            </Button>
          )}
          <Button
            onClick={async () => {
              // Create staged new tier first, then include its ID in the vault save.
              let newTierIds = [...edit.tierIds];
              if (newTier && isTierComplete(newTier, cloudServiceOptions.length > 0)) {
                const tierId = crypto.randomUUID();
                const tierCfg = new TierConfig({
                  id: tierId,
                  name: newTier.type,
                  type: tierTypeEnum(newTier.type),
                  storageClass: newTier.type === "file" ? parseInt(newTier.storageClass, 10) || 0 : 0,
                  cloudServiceId: newTier.type === "cloud" ? newTier.cloudServiceId : "",
                  activeChunkClass: newTier.type === "cloud" ? parseInt(newTier.activeChunkClass, 10) || 0 : 0,
                  cacheClass: newTier.type === "cloud" ? parseInt(newTier.cacheClass, 10) || 0 : 0,
                  memoryBudgetBytes: newTier.type === "memory" ? parseMemoryBudget(newTier.memoryBudget) : protoInt64.zero,
                  rotationPolicyId: newTier.rotationPolicyId,
                  retentionRules: newTier.retentionPolicyId
                    ? [new RetentionRule({
                        retentionPolicyId: newTier.retentionPolicyId,
                        action: retentionActionForPosition(vaultTiers.length, vaultTiers.length + 1),
                      })]
                    : [],
                  replicationFactor: parseInt(newTier.replicationFactor, 10) || 1,
                });
                try {
                  await putTier.mutateAsync({ config: tierCfg });
                  newTierIds = [...newTierIds, tierId];
                  setEdit({ tierIds: newTierIds });
                  setNewTier(null);
                } catch (err: unknown) {
                  addToast(err instanceof Error ? err.message : "Failed to create tier", "error");
                  return; // Don't save vault if tier creation failed.
                }
              }

              // Save tier-level changes (rotation + retention) via PutTier.
              for (const [tierId, rpId] of Object.entries(edit.tierRotation)) {
                const tier = tiers.find((t) => t.id === tierId);
                if (!tier || rpId === tier.rotationPolicyId) continue;
                const updated = tier.clone();
                updated.rotationPolicyId = rpId;
                // Also apply retention for this tier in the same PutTier call.
                const retPolicyId = edit.tierRetention[tierId] ?? "";
                const tierIndex = edit.tierIds.indexOf(tierId);
                updated.retentionRules = retPolicyId
                  ? [new RetentionRule({ retentionPolicyId: retPolicyId, action: retentionActionForPosition(tierIndex, edit.tierIds.length) })]
                  : [];
                await putTier.mutateAsync({ config: updated });
              }
              // Save retention-only changes (tiers whose rotation didn't change).
              for (const [tierId, retPolicyId] of Object.entries(edit.tierRetention)) {
                if (tierId in edit.tierRotation) {
                  const tier = tiers.find((t) => t.id === tierId);
                  if (tier && edit.tierRotation[tierId] !== tier.rotationPolicyId) continue; // already saved above
                }
                const tier = tiers.find((t) => t.id === tierId);
                if (!tier) continue;
                const currentRetId = tier.retentionRules[0]?.retentionPolicyId ?? "";
                if (retPolicyId === currentRetId) continue;
                const tierIndex = edit.tierIds.indexOf(tierId);
                const updated = tier.clone();
                updated.retentionRules = retPolicyId
                  ? [new RetentionRule({ retentionPolicyId: retPolicyId, action: retentionActionForPosition(tierIndex, edit.tierIds.length) })]
                  : [];
                await putTier.mutateAsync({ config: updated });
              }
              // Save RF changes (parse string → number, empty/invalid defaults to 1).
              for (const [tierId, rfStr] of Object.entries(edit.tierRF)) {
                const tier = tiers.find((t) => t.id === tierId);
                const rf = parseInt(rfStr, 10) || 1;
                if (!tier || rf === (tier.replicationFactor || 1)) continue;
                const updated = tier.clone();
                updated.replicationFactor = rf;
                await putTier.mutateAsync({ config: updated });
              }
              // Save storage class changes.
              for (const [tierId, scStr] of Object.entries(edit.tierStorageClass)) {
                const tier = tiers.find((t) => t.id === tierId);
                const sc = parseInt(scStr, 10) || 0;
                if (!tier || sc === (tier.storageClass || 0)) continue;
                const updated = tier.clone();
                updated.storageClass = sc;
                await putTier.mutateAsync({ config: updated });
              }
              // Save vault-level changes (name, enabled, tier order).
              const vaultChanged = edit.name !== vault.name || edit.enabled !== vault.enabled ||
                JSON.stringify(newTierIds) !== JSON.stringify([...vault.tierIds]);
              if (vaultChanged) {
                saveVault(vault.id, { name: edit.name, tierIds: newTierIds, enabled: edit.enabled });
              }
              // Mark for reset — the next props update will sync edit state.
              setPendingReset(true);
            }}
            disabled={putVault.isPending || putTier.isPending || !anyDirty}
          >
            {putVault.isPending || putTier.isPending ? "Saving..." : "Save"}
          </Button>
        </>
      }
      headerRight={
        <span className="flex items-center gap-2">
          {!vault.enabled && (
            <Badge variant="ghost" dark={dark}>disabled</Badge>
          )}
          {warnings.length > 0 && (
            <span className="text-[0.85em] text-severity-warn">
              {warnings.join(", ")}
            </span>
          )}
          {onOpenInspector && (
            <CrossLinkBadge dark={dark} title="Open in Inspector" onClick={() => onOpenInspector(`entities:vaults:${vault.name || vault.id}`)}>
              <PulseIcon className="w-3 h-3" />
            </CrossLinkBadge>
          )}
        </span>
      }
    >
      <div className="flex flex-col gap-3">
        <FormField label="Name" dark={dark}>
          <TextInput
            value={edit.name}
            onChange={(v) => setEdit({ name: v })}
            dark={dark}
          />
        </FormField>
        <Checkbox
          checked={edit.enabled}
          onChange={(v) => setEdit({ enabled: v })}
          label="Enabled"
          dark={dark}
        />
        {/* Tier list */}
        <div className="flex flex-col gap-2">
          <span className={`text-[0.75em] font-medium uppercase tracking-[0.12em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            Tiers
          </span>
          {vaultTiers.length === 0 && !newTier && (
            <span className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
              No tiers assigned.
            </span>
          )}
          {vaultTiers.length > 0 && (
            <div className="flex flex-col gap-1.5">
              {vaultTiers.map((tier, i) => {
                const nodeName = tier.nodeId ? resolveNodeName(tier.nodeId) : null;
                const csName = tier.cloudServiceId
                  ? cloudServiceOptions.find((cs) => cs.value === tier.cloudServiceId)?.label || tier.cloudServiceId
                  : null;
                return (
                  <div
                    key={tier.id}
                    className={`flex flex-col gap-1 px-3 py-2 rounded border ${c(
                      "border-ink-border/60 bg-ink-base/40",
                      "border-light-border/60 bg-light-base/40",
                    )}`}
                  >
                    <div className="flex items-center gap-3">
                      <span className={`text-[0.7em] font-mono tabular-nums ${c("text-text-ghost", "text-light-text-ghost")}`}>
                        {i + 1}
                      </span>
                      <Badge variant="copper" dark={dark}>
                        {tierTypeLabel(tier.type)}
                      </Badge>
                      {i === 0 && (
                        <span className={`text-[0.7em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
                          hottest
                        </span>
                      )}
                      <span className="flex-1" />
                      <div className="flex items-center gap-1">
                        <button
                          onClick={() => {
                            if (i === 0) return;
                            const ids = [...localTierIds];
                            const tmp = ids[i - 1]!; ids[i - 1] = ids[i]!; ids[i] = tmp;
                            setLocalTierIds(ids);
                          }}
                          disabled={i === 0}
                          className={`px-1 py-0.5 text-[0.75em] rounded transition-colors ${c(
                            "text-text-ghost hover:text-text-bright hover:bg-ink-hover",
                            "text-light-text-ghost hover:text-light-text-bright hover:bg-light-hover",
                          )} disabled:opacity-20 disabled:pointer-events-none`}
                          title="Move up"
                        >
                          {"\u25B2"}
                        </button>
                        <button
                          onClick={() => {
                            if (i >= vaultTiers.length - 1) return;
                            const ids = [...localTierIds];
                            const tmp = ids[i]!; ids[i] = ids[i + 1]!; ids[i + 1] = tmp;
                            setLocalTierIds(ids);
                          }}
                          disabled={i === vaultTiers.length - 1}
                          className={`px-1 py-0.5 text-[0.75em] rounded transition-colors ${c(
                            "text-text-ghost hover:text-text-bright hover:bg-ink-hover",
                            "text-light-text-ghost hover:text-light-text-bright hover:bg-light-hover",
                          )} disabled:opacity-20 disabled:pointer-events-none`}
                          title="Move down"
                        >
                          {"\u25BC"}
                        </button>
                      </div>
                      <button
                        onClick={() => setLocalTierIds(localTierIds.filter((id) => id !== tier.id))}
                        className={`px-2 py-1 text-[0.75em] rounded transition-colors ${c(
                          "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
                          "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
                        )}`}
                      >
                        Remove
                      </button>
                    </div>
                    <div className={`flex items-center gap-3 pl-6 text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                      {nodeName && <span>{"node: " + nodeName}</span>}
                      {!nodeName && <span className={c("text-text-ghost", "text-light-text-ghost")}>unplaced</span>}
                      {tier.type === TierType.FILE && tier.storageClass > 0 && (
                        <span className="font-mono">{`class ${String(tier.storageClass)}`}</span>
                      )}
                      {tier.type === TierType.MEMORY && tier.memoryBudgetBytes > 0 && (
                        <span className="font-mono">{formatBytes(tier.memoryBudgetBytes)}</span>
                      )}
                      {tier.type === TierType.CLOUD && csName && <span>{csName}</span>}
                      {tier.type === TierType.CLOUD && tier.activeChunkClass > 0 && (
                        <span className="font-mono">{`chunk class ${String(tier.activeChunkClass)}`}</span>
                      )}
                      <span>{`RF=${String(tier.replicationFactor || 1)}`}</span>
                      {(tier.secondaryNodeIds?.length ?? 0) > 0 && (
                        <span>
                          {tier.secondaryNodeIds.map((id, si) => {
                            const name = resolveNodeName(id);
                            const sc = tier.storageClass > 0 ? nodeStorageClass(id, tier.storageClass) : null;
                            const fallback = sc && !sc.exact && sc.actualClass > 0;
                            return (
                              <span key={id}>
                                {si > 0 && ", "}
                                {name}
                                {fallback && (
                                  <span className="text-severity-warn">{` (class ${String(sc.actualClass)})`}</span>
                                )}
                              </span>
                            );
                          })}
                        </span>
                      )}
                      {(tier.replicationFactor || 1) > 1 && (tier.secondaryNodeIds?.length ?? 0) + 1 < (tier.replicationFactor || 1) && (
                        <span className="text-severity-error">
                          {`insufficient nodes for RF=${String(tier.replicationFactor)}`}
                        </span>
                      )}
                    </div>
                    <div className="pl-6 flex flex-col gap-2">
                      {rotationPolicyOptions.length > 0 && (
                        <FormField label="Rotation Policy" dark={dark}>
                          <SelectInput
                            value={getTierRotationPolicy(tier.id)}
                            onChange={(v) => setTierRotationPolicy(tier.id, v)}
                            options={[
                              { value: "", label: "None" },
                              ...rotationPolicyOptions,
                            ]}
                            dark={dark}
                          />
                        </FormField>
                      )}
                      {retentionPolicyOptions.length > 0 && (
                        <>
                          <FormField label="Retention Policy" dark={dark}>
                            <SelectInput
                              value={getTierRetentionPolicyId(tier.id)}
                              onChange={(v) => setTierRetentionPolicyId(tier.id, v)}
                              options={[
                                { value: "", label: "None" },
                                ...retentionPolicyOptions,
                              ]}
                              dark={dark}
                            />
                          </FormField>
                        </>
                      )}
                      {tier.type === TierType.FILE && storageClassOptions.length > 0 && (
                        <FormField label="Storage Class" dark={dark}>
                          <SelectInput
                            value={edit.tierStorageClass[tier.id] ?? String(tier.storageClass || 0)}
                            onChange={(v) => setTierStorageClass(tier.id, v)}
                            options={storageClassOptions}
                            dark={dark}
                          />
                        </FormField>
                      )}
                      <FormField label="Replication Factor" dark={dark}>
                        <NumberInput
                          value={edit.tierRF[tier.id] ?? String(tier.replicationFactor ?? 1)}
                          onChange={(v) => setTierRF(tier.id, v)}
                          placeholder="1"
                          dark={dark}
                          min={1}
                        />
                      </FormField>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          {newTier ? (
            <TierEntryCard
              tier={newTier}
              index={vaultTiers.length}
              dark={dark}
              storageClassOptions={storageClassOptions}
              cloudServiceOptions={cloudServiceOptions}
              rotationPolicyOptions={rotationPolicyOptions}
              retentionPolicyOptions={retentionPolicyOptions}
              onUpdate={(patch) => setNewTier((t) => t ? { ...t, ...patch } : t)}
              onRemove={() => setNewTier(null)}
            />
          ) : (
            <div className="flex justify-end">
              <DropdownButton
                label="+ Add Tier"
                items={[
                  { value: "memory", label: "Memory" },
                  { value: "file", label: "File" },
                  { value: "cloud", label: "Cloud" },
                ]}
                onSelect={(v) => setNewTier(emptyTierEntry(v as TierTypeLabel))}
                dark={dark}
                dropUp
              />
            </div>
          )}
        </div>
        {migrateTarget && (
          <MigrateVaultForm
            dark={dark}
            vault={vault}
            target={migrateTarget}
            isPending={migrate.isPending}
            activeJob={!!activeJob}
            onTargetChange={(update) =>
              setMigrateTarget((prev) => prev ? { ...prev, ...update } : prev)
            }
            onSubmit={async () => {
              const trimmedName = migrateTarget.name.trim();
              if (!trimmedName) return;
              const srcLabel = vault.name || vault.id;
              if (!confirm(`Migrate "${srcLabel}" to "${trimmedName}"? This will immediately disable "${srcLabel}" and delete it after all records are moved.`)) return;
              const destType = migrateTarget.type || undefined;
              const params: Record<string, string> = {};
              if (migrateTarget.dir.trim()) {
                params["dir"] = migrateTarget.dir.trim();
              }
              const destParams = Object.keys(params).length > 0 ? params : undefined;
              try {
                const result = await migrate.mutateAsync({
                  source: vault.id,
                  destination: trimmedName,
                  destinationType: destType,
                  destinationParams: destParams,
                });
                setActiveJob({ jobId: result.jobId, label: "Migrating" });
                setMigrateTarget(null);
              } catch (err: unknown) {
                addToast(err instanceof Error ? err.message : "Migrate failed", "error");
              }
            }}
          />
        )}
        {mergeTarget !== null && (
          <MergeVaultForm
            dark={dark}
            vault={vault}
            selectedDestination={mergeTarget}
            vaults={vaults}
            isPending={merge.isPending}
            activeJob={!!activeJob}
            onDestinationChange={setMergeTarget}
            onSubmit={async () => {
              if (!mergeTarget) return;
              const destName = vaults.find((s) => s.id === mergeTarget)?.name || mergeTarget;
              if (!confirm(`Merge "${vault.name || vault.id}" into "${destName}"? This will immediately disable "${vault.name || vault.id}" and delete it after all records are moved.`)) return;
              try {
                const result = await merge.mutateAsync({
                  source: vault.id,
                  destination: mergeTarget,
                });
                setActiveJob({ jobId: result.jobId, label: "Merging" });
                setMergeTarget(null);
              } catch (err: unknown) {
                addToast(err instanceof Error ? err.message : "Merge failed", "error");
              }
            }}
          />
        )}
      </div>
    </SettingsCard>
  );
}
