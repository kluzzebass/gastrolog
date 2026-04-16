import { encode, decode } from "../../api/glid";
import { useState } from "react";
import { protoInt64 } from "@bufbuild/protobuf";
import type { VaultConfig, RouteConfig, NodeConfig } from "../../api/gen/gastrolog/v1/system_pb";
import type { NodeStorageConfig } from "../../api/gen/gastrolog/v1/storage_pb";
import { TierConfig, TierType, RetentionRule } from "../../api/gen/gastrolog/v1/system_pb";
import {
  usePutVault,
  useDeleteVault,
  useDeleteTier,
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
import { FormField, TextInput, SelectInput, SpinnerInput } from "./FormField";
import { Button, DropdownButton } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { PulseIcon } from "../icons";
import { CrossLinkBadge } from "../inspector/CrossLinkBadge";
import { JobProgress } from "./VaultHelpers";
import { MigrateVaultForm, MergeVaultForm } from "./VaultMigrateForms";
import { useThemeClass } from "../../hooks/useThemeClass";
import { leaderNodeId, followerNodeIds } from "../../utils/tierPlacement";
import { buildNodeNameMap, resolveNodeName } from "../../utils/nodeNames";
import { formatBytes } from "../../utils/units";


import {
  TierEntryCard,
  emptyTierEntry,
  tierTypeEnum,
  tierTypeLabel,
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
  // Compute max RF per storage class — count file storages, not nodes.
  const classStorageCount = new Map<number, number>();
  for (const nsc of nodeStorageConfigs) {
    for (const fs of nsc.fileStorages) {
      classStorageCount.set(fs.storageClass, (classStorageCount.get(fs.storageClass) ?? 0) + 1);
    }
  }
  const totalNodes = nodeConfigs.length || 1;
  const maxRFForTier = (t: { type: TierType; storageClass: number; activeChunkClass: number }) => {
    if (t.type === TierType.MEMORY) return totalNodes;
    if (t.type === TierType.JSONL) return 1;
    const sc = t.type === TierType.CLOUD ? t.activeChunkClass : t.storageClass;
    if (sc === 0) return 1; // no class selected yet
    return classStorageCount.get(sc) ?? 1;
  };
  const c = useThemeClass(dark);
  const putVault = usePutVault();
  const putTier = usePutTier();
  const deleteTier = useDeleteTier();
  const deleteVault = useDeleteVault();
  const [confirmRemoveTier, setConfirmRemoveTier] = useState<string | null>(null);
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

  // Derive vault's tiers from the global tier list, sorted by position.
  const vaultIdStr = encode(vault.id);
  const derivedVaultTierIds = tiers
    .filter((t) => encode(t.vaultId) === vaultIdStr)
    .toSorted((a, b) => a.position - b.position)
    .map((t) => encode(t.id));

  // All vault+tier edits in one place. Initialized from props, reset on cancel/save.
  interface TierRemoval {
    tierId: string;
    drain: boolean; // true = drain to next tier, false = delete data immediately
  }

  interface TierEditState {
    rotationPolicyId: string;
    retentionPolicyId: string;
    replicationFactor: string;
    storageClass: string;
    activeChunkClass: string;
    cacheClass: string;
    cacheEviction: string;
    cacheBudget: string;
    cacheTTL: string;
  }

  const emptyTierEdit: TierEditState = {
    rotationPolicyId: "",
    retentionPolicyId: "",
    replicationFactor: "1",
    storageClass: "0",
    activeChunkClass: "0",
    cacheClass: "0",
    cacheEviction: "lru",
    cacheBudget: "",
    cacheTTL: "",
  };

  interface VaultEdit {
    name: string;
    enabled: boolean;
    tierIds: string[];
    tierRemovals: TierRemoval[];
    tierEdits: Record<string, TierEditState>;
  }

  const vaultTierConfigs = tiers.filter((t) => encode(t.vaultId) === vaultIdStr);

  const buildInitialEdit = (): VaultEdit => ({
    name: vault.name,
    enabled: vault.enabled,
    tierIds: [...derivedVaultTierIds],
    tierRemovals: [],
    tierEdits: Object.fromEntries(
      vaultTierConfigs.map((t) => [encode(t.id), {
        rotationPolicyId: encode(t.rotationPolicyId),
        retentionPolicyId: t.retentionRules[0] ? encode(t.retentionRules[0].retentionPolicyId) : "",
        replicationFactor: String(t.replicationFactor || 1),
        storageClass: String(t.storageClass || 0),
        activeChunkClass: String(t.activeChunkClass || 0),
        cacheClass: String(t.cacheClass || 0),
        cacheEviction: t.cacheEviction || "lru",
        cacheBudget: t.cacheBudget || "",
        cacheTTL: t.cacheTtl || "",
      } satisfies TierEditState]),
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
  const setTierField = (tierId: string, field: keyof TierEditState, value: string) =>
    setEditState((prev) => ({
      ...prev,
      tierEdits: {
        ...prev.tierEdits,
        [tierId]: { ...emptyTierEdit, ...prev.tierEdits[tierId], [field]: value },
      },
    }));

  const anyDirty = JSON.stringify(edit) !== initialJson || newTier !== null;

  // Aliases for backward compat with the rest of the component.
  const localTierIds = edit.tierIds;
  const setLocalTierIds = (ids: string[]) => setEdit({ tierIds: ids });
  const getTierRotationPolicy = (tierId: string): string => edit.tierEdits[tierId]?.rotationPolicyId ?? "";
  const getTierRetentionPolicyId = (tierId: string): string => edit.tierEdits[tierId]?.retentionPolicyId ?? "";

  // Resolve vault's tiers from local state (reflects unsaved reorder/remove).
  const tierMap = new Map(tiers.map((t) => [encode(t.id), t]));
  const vaultTiers = localTierIds.map((id) => tierMap.get(id)).filter((t): t is TierConfig => !!t);

  // Node name resolution for tier placement display.
  const nodeNameMap = buildNodeNameMap(nodeConfigs);

  // Check if a node has a specific storage class; returns the fallback class if not.
  const nodeStorageClass = (nodeId: string, requiredClass: number): { exact: boolean; actualClass: number } => {
    const nsc = nodeStorageConfigs.find((n) => encode(n.nodeId) === nodeId);
    if (!nsc) return { exact: false, actualClass: 0 };
    if (nsc.fileStorages.some((a) => a.storageClass === requiredClass)) return { exact: true, actualClass: requiredClass };
    const first = nsc.fileStorages[0];
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
        enabled: boolean;
      },
    ) => ({
      id,
      name: e.name,
      enabled: e.enabled,
    }),
    onDeleteTransform: (id) => ({ id, force: true, deleteData }),
    // Don't reset edit state eagerly — props are stale inside the async
    // handler. The edit state stays as-is; anyDirty becomes false naturally
    // when the parent re-renders with updated config from setQueryData.
  });

  // ── handleSaveAll phases ───────────────────────────────────────────

  /** Create a staged new tier and return the updated tier ID list, or null on failure. */
  const createStagedTier = async (tierIds: string[]): Promise<string[] | null> => {
    if (!newTier || !isTierComplete(newTier, cloudServiceOptions.length > 0)) return tierIds;
    const tierCfg = new TierConfig({
      name: newTier.type,
      type: tierTypeEnum(newTier.type),
      vaultId: vault.id,  // already Uint8Array from proto
      position: tierIds.length,
      storageClass: newTier.type === "file" ? parseInt(newTier.storageClass, 10) || 0 : 0,
      cloudServiceId: newTier.type === "cloud" && newTier.cloudServiceId ? decode(newTier.cloudServiceId) : new Uint8Array(0),
      activeChunkClass: newTier.type === "cloud" ? parseInt(newTier.activeChunkClass, 10) || 0 : 0,
      cacheClass: newTier.type === "cloud" ? parseInt(newTier.cacheClass, 10) || 0 : 0,
      cacheEviction: newTier.type === "cloud" ? (newTier.cacheEviction || "lru") : "",
      cacheBudget: newTier.type === "cloud" ? (newTier.cacheBudget || "") : "",
      cacheTtl: newTier.type === "cloud" ? (newTier.cacheTTL || "") : "",
      memoryBudgetBytes: newTier.type === "memory" ? parseMemoryBudget(newTier.memoryBudget) : protoInt64.zero,
      rotationPolicyId: newTier.rotationPolicyId ? decode(newTier.rotationPolicyId) : new Uint8Array(0),
      retentionRules: newTier.retentionPolicyId
        ? [new RetentionRule({
            retentionPolicyId: decode(newTier.retentionPolicyId),
            action: retentionActionForPosition(vaultTiers.length, vaultTiers.length + 1),
          })]
        : [],
      replicationFactor: newTier.type === "jsonl" ? 1 : parseInt(newTier.replicationFactor, 10) || 1,
      path: newTier.type === "jsonl" ? newTier.path : "",
    });
    try {
      await putTier.mutateAsync({ config: tierCfg });
      // Server assigns the tier ID; the config cache refresh after mutation will pick it up.
      // Return current tierIds unchanged — the pending reset will sync on next render.
      return tierIds;
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create tier", "error");
      return null;
    }
  };

  /** Save per-tier changes (rotation, retention, RF, storage, cache, position). Returns true if all succeeded. */
  const updateExistingTiers = async (tierIds: string[]): Promise<boolean> => {
    let ok = true;
    for (let tierIndex = 0; tierIndex < tierIds.length; tierIndex++) {
      const tierId = tierIds[tierIndex]!;
      const tier = tiers.find((t) => encode(t.id) === tierId);
      if (!tier) continue;

      const rpId = edit.tierEdits[tierId]?.rotationPolicyId ?? encode(tier.rotationPolicyId);
      const retPolicyId = edit.tierEdits[tierId]?.retentionPolicyId ?? (tier.retentionRules[0] ? encode(tier.retentionRules[0].retentionPolicyId) : "");
      const rfStr = edit.tierEdits[tierId]?.replicationFactor ?? String(tier.replicationFactor || 1);
      const rf = parseInt(rfStr, 10) || 1;
      const scStr = edit.tierEdits[tierId]?.storageClass ?? String(tier.storageClass || 0);
      const sc = parseInt(scStr, 10) || 0;
      const accStr = edit.tierEdits[tierId]?.activeChunkClass ?? String(tier.activeChunkClass || 0);
      const acc = parseInt(accStr, 10) || 0;
      const ccStr = edit.tierEdits[tierId]?.cacheClass ?? String(tier.cacheClass || 0);
      const cc = parseInt(ccStr, 10) || 0;
      const ceStr = edit.tierEdits[tierId]?.cacheEviction ?? (tier.cacheEviction || "lru");
      const cbStr = edit.tierEdits[tierId]?.cacheBudget ?? (tier.cacheBudget || "");
      const ctStr = edit.tierEdits[tierId]?.cacheTTL ?? (tier.cacheTtl || "");

      const expectedAction = retentionActionForPosition(tierIndex, tierIds.length);
      const currentAction = tier.retentionRules[0]?.action;
      const currentRetId = tier.retentionRules[0] ? encode(tier.retentionRules[0].retentionPolicyId) : "";

      const rotChanged = rpId !== encode(tier.rotationPolicyId);
      const retChanged = retPolicyId !== currentRetId || (retPolicyId && currentAction !== expectedAction);
      const rfChanged = rf !== (tier.replicationFactor || 1);
      const scChanged = sc !== (tier.storageClass || 0);
      const accChanged = acc !== (tier.activeChunkClass || 0);
      const ccChanged = cc !== (tier.cacheClass || 0);
      const ceChanged = ceStr !== (tier.cacheEviction || "lru");
      const cbChanged = cbStr !== (tier.cacheBudget || "");
      const ctChanged = ctStr !== (tier.cacheTtl || "");
      const posChanged = tier.position !== tierIndex;
      const vaultIdChanged = tier.vaultId !== vault.id;

      if (!rotChanged && !retChanged && !rfChanged && !scChanged && !accChanged && !ccChanged && !ceChanged && !cbChanged && !ctChanged && !posChanged && !vaultIdChanged) continue;

      const updated = tier.clone();
      if (rotChanged) updated.rotationPolicyId = rpId ? decode(rpId) : new Uint8Array(0);
      if (rfChanged) updated.replicationFactor = rf;
      if (scChanged) updated.storageClass = sc;
      if (accChanged) updated.activeChunkClass = acc;
      if (ccChanged) updated.cacheClass = cc;
      if (ceChanged) updated.cacheEviction = ceStr;
      if (cbChanged) updated.cacheBudget = cbStr || "";
      if (ctChanged) updated.cacheTtl = ctStr;
      updated.vaultId = vault.id;
      updated.position = tierIndex;
      updated.retentionRules = retPolicyId
        ? [new RetentionRule({ retentionPolicyId: decode(retPolicyId), action: expectedAction })]
        : [];
      try {
        await putTier.mutateAsync({ config: updated });
      } catch (err: unknown) {
        addToast(err instanceof Error ? err.message : `Failed to update tier "${tier.name}"`, "error");
        ok = false;
      }
    }
    return ok;
  };

  /** Execute staged tier removals (drain or delete). */
  const executeRemovals = async () => {
    for (const removal of edit.tierRemovals) {
      try {
        await deleteTier.mutateAsync({ id: removal.tierId, drain: removal.drain });
      } catch (err: unknown) {
        const action = removal.drain ? "drain" : "delete";
        const msg = err instanceof Error ? err.message : `Failed to ${action} tier`;
        addToast(msg, "error");
      }
    }
  };

  const handleSaveAll = async () => {
    const tierIds = await createStagedTier([...edit.tierIds]);
    if (!tierIds) return;
    await updateExistingTiers(tierIds);
    await executeRemovals();
    if (edit.name !== vault.name || edit.enabled !== vault.enabled) {
      await saveVault(encode(vault.id), { name: edit.name, enabled: edit.enabled });
    }
    // Clear the new-tier form and let the pending reset sync with server
    // state once the config cache refreshes (includes the newly created tier).
    setNewTier(null);
    setPendingReset(true);
  };

  const warnings: string[] = [];
  if (vaultTiers.length === 0) warnings.push("no tiers configured");

  return (
    <SettingsCard
      key={encode(vault.id)}
      id={vault.name || encode(vault.id)}
      typeBadge={vaultTiers.length > 0 ? vaultTiers.map((t) => tierTypeLabel(t.type)).join(", ") : undefined}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={() => handleDelete(encode(vault.id))}
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
                await seal.mutateAsync(encode(vault.id));
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
                const result = await reindex.mutateAsync(encode(vault.id));
                setActiveJob({ jobId: encode(result.jobId), label: "Reindexing" });
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
              Discard
            </Button>
          )}
          <Button
            onClick={handleSaveAll}
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
            <CrossLinkBadge dark={dark} title="Open in Inspector" onClick={() => onOpenInspector(`entities:vaults:${vault.name || encode(vault.id)}`)}>
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
                const pnId = leaderNodeId(tier, nodeStorageConfigs);
                const nodeName = pnId ? resolveNodeName(nodeNameMap, pnId) : null;
                const csName = tier.cloudServiceId
                  ? cloudServiceOptions.find((cs) => cs.value === encode(tier.cloudServiceId))?.label || encode(tier.cloudServiceId)
                  : null;
                return (
                  <div
                    key={encode(tier.id)}
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
                      {confirmRemoveTier === encode(tier.id) ? (
                        <div className="flex items-center gap-1.5">
                          <span className={`text-[0.7em] ${c("text-text-muted", "text-light-text-muted")}`}>
                            Remove tier?
                          </span>
                          {i < vaultTiers.length - 1 && (
                            <button
                              onClick={() => {
                                setEdit({
                                  tierIds: localTierIds.filter((id) => id !== encode(tier.id)),
                                  tierRemovals: [...edit.tierRemovals, { tierId: encode(tier.id), drain: true }],
                                });
                                setConfirmRemoveTier(null);
                              }}
                              className="px-2 py-1 text-[0.7em] rounded bg-copper/15 text-copper hover:bg-copper/25 transition-colors"
                            >
                              Drain
                            </button>
                          )}
                          <button
                            onClick={() => {
                              setEdit({
                                tierIds: localTierIds.filter((id) => id !== encode(tier.id)),
                                tierRemovals: [...edit.tierRemovals, { tierId: encode(tier.id), drain: false }],
                              });
                              setConfirmRemoveTier(null);
                            }}
                            className="px-2 py-1 text-[0.7em] rounded bg-severity-error/15 text-severity-error hover:bg-severity-error/25 transition-colors"
                          >
                            Delete
                          </button>
                          <button
                            onClick={() => setConfirmRemoveTier(null)}
                            className={`px-2 py-1 text-[0.7em] rounded transition-colors ${c(
                              "text-text-muted hover:bg-ink-hover",
                              "text-light-text-muted hover:bg-light-hover",
                            )}`}
                          >
                            Cancel
                          </button>
                        </div>
                      ) : (
                        <button
                          onClick={() => setConfirmRemoveTier(encode(tier.id))}
                          className={`px-2 py-1 text-[0.75em] rounded transition-colors ${c(
                            "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
                            "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
                          )}`}
                        >
                          Remove
                        </button>
                      )}
                    </div>
                    <div className={`flex items-center gap-3 pl-6 text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                      {nodeName && <span>{"node: " + nodeName}</span>}
                      {!nodeName && <span className={c("text-text-ghost", "text-light-text-ghost")}>unplaced</span>}
                      {tier.type === TierType.FILE && tier.storageClass > 0 && (
                        <span className="font-mono">{`class ${String(tier.storageClass)}`}</span>
                      )}
                      {tier.type === TierType.JSONL && (
                        <span className="font-mono">
                          {tier.path || `jsonl/${vault.name || "vault"}/sink_${String(i + 1)}.jsonl`}
                        </span>
                      )}
                      {tier.type === TierType.MEMORY && tier.memoryBudgetBytes > 0 && (
                        <span className="font-mono">{formatBytes(tier.memoryBudgetBytes)}</span>
                      )}
                      {tier.type === TierType.CLOUD && csName && <span>{csName}</span>}
                      {tier.type === TierType.CLOUD && tier.activeChunkClass > 0 && (
                        <span className="font-mono">{`chunk class ${String(tier.activeChunkClass)}`}</span>
                      )}
                      {tier.type !== TierType.JSONL && (
                        <span>{`RF=${String(tier.replicationFactor || 1)}`}</span>
                      )}
                      {followerNodeIds(tier, nodeStorageConfigs).length > 0 && (
                        <span>
                          {followerNodeIds(tier, nodeStorageConfigs).map((id: string, si: number) => {
                            const name = resolveNodeName(nodeNameMap, id);
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
                      {(tier.replicationFactor || 1) > 1 && followerNodeIds(tier, nodeStorageConfigs).length + 1 < (tier.replicationFactor || 1) && (
                        <span className="text-severity-error">
                          {`insufficient nodes for RF=${String(tier.replicationFactor)}`}
                        </span>
                      )}
                    </div>
                    <div className="pl-6 flex flex-col gap-2">
                      {tier.type !== TierType.JSONL && rotationPolicyOptions.length > 0 && (
                        <FormField label="Rotation Policy" dark={dark}>
                          <SelectInput
                            value={getTierRotationPolicy(encode(tier.id))}
                            onChange={(v) => setTierField(encode(tier.id), "rotationPolicyId", v)}
                            options={[
                              { value: "", label: "None" },
                              ...rotationPolicyOptions,
                            ]}
                            dark={dark}
                          />
                        </FormField>
                      )}
                      {tier.type !== TierType.JSONL && retentionPolicyOptions.length > 0 && (
                        <>
                          <FormField label="Retention Policy" dark={dark}>
                            <SelectInput
                              value={getTierRetentionPolicyId(encode(tier.id))}
                              onChange={(v) => setTierField(encode(tier.id), "retentionPolicyId", v)}
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
                            value={edit.tierEdits[encode(tier.id)]?.storageClass ?? String(tier.storageClass || 0)}
                            onChange={(v) => setTierField(encode(tier.id), "storageClass", v)}
                            options={storageClassOptions}
                            dark={dark}
                          />
                        </FormField>
                      )}
                      {tier.type === TierType.CLOUD && storageClassOptions.length > 0 && (
                        <div className="grid grid-cols-2 gap-2">
                          <FormField label="Active Chunk Class" dark={dark}>
                            <SelectInput
                              value={edit.tierEdits[encode(tier.id)]?.activeChunkClass ?? String(tier.activeChunkClass || 0)}
                              onChange={(v) => setTierField(encode(tier.id), "activeChunkClass", v)}
                              options={[{ value: "0", label: "None" }, ...storageClassOptions]}
                              dark={dark}
                            />
                          </FormField>
                          <FormField label="Cache Class" dark={dark}>
                            <SelectInput
                              value={edit.tierEdits[encode(tier.id)]?.cacheClass ?? String(tier.cacheClass || 0)}
                              onChange={(v) => setTierField(encode(tier.id), "cacheClass", v)}
                              options={[{ value: "0", label: "None" }, ...storageClassOptions]}
                              dark={dark}
                            />
                          </FormField>
                        </div>
                      )}
                      {tier.type === TierType.CLOUD && parseInt(edit.tierEdits[encode(tier.id)]?.cacheClass ?? String(tier.cacheClass || 0), 10) > 0 && (
                        <>
                          <FormField label="Cache Eviction" dark={dark}>
                            <SelectInput
                              value={edit.tierEdits[encode(tier.id)]?.cacheEviction ?? (tier.cacheEviction || "lru")}
                              onChange={(v) => setTierField(encode(tier.id), "cacheEviction", v)}
                              options={[
                                { value: "lru", label: "LRU — evict oldest when over budget" },
                                { value: "ttl", label: "TTL — evict after max age" },
                              ]}
                              dark={dark}
                            />
                          </FormField>
                          <FormField label="Cache Budget" dark={dark}>
                            <TextInput
                              value={edit.tierEdits[encode(tier.id)]?.cacheBudget ?? (tier.cacheBudget || "")}
                              onChange={(v) => setTierField(encode(tier.id), "cacheBudget", v)}
                              placeholder="1GiB"
                              dark={dark}
                              mono
                              examples={["500MB", "1GiB", "5GB", "10GB"]}
                            />
                          </FormField>
                          {(edit.tierEdits[encode(tier.id)]?.cacheEviction ?? (tier.cacheEviction || "lru")) === "ttl" && (
                            <FormField label="Cache TTL" dark={dark}>
                              <TextInput
                                value={edit.tierEdits[encode(tier.id)]?.cacheTTL ?? (tier.cacheTtl || "")}
                                onChange={(v) => setTierField(encode(tier.id), "cacheTTL", v)}
                                placeholder=""
                                dark={dark}
                                mono
                                examples={["1h", "12h", "1d", "7d"]}
                              />
                            </FormField>
                          )}
                        </>
                      )}
                      {tier.type !== TierType.JSONL && (
                        <FormField label="Replication Factor" dark={dark} description="1 = none, 2 = redundant, 3+ = fault tolerant">
                          <SpinnerInput
                            value={edit.tierEdits[encode(tier.id)]?.replicationFactor ?? String(tier.replicationFactor || 1)}
                            onChange={(v) => setTierField(encode(tier.id), "replicationFactor", v)}
                            dark={dark}
                            min={1}
                            max={maxRFForTier({
                              type: tier.type,
                              storageClass: parseInt(edit.tierEdits[encode(tier.id)]?.storageClass ?? String(tier.storageClass || 0), 10) || 0,
                              activeChunkClass: parseInt(edit.tierEdits[encode(tier.id)]?.activeChunkClass ?? String(tier.activeChunkClass || 0), 10) || 0,
                            })}
                          />
                        </FormField>
                      )}
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
              nodeOptions={nodeConfigs.map((n) => ({ value: encode(n.id), label: n.name || encode(n.id) })).sort((a, b) => a.label.localeCompare(b.label))}
              vaultName={vault.name || ""}
              maxRF={maxRFForTier({
                type: tierTypeEnum(newTier.type),
                storageClass: parseInt(newTier.storageClass, 10) || 0,
                activeChunkClass: parseInt(newTier.activeChunkClass, 10) || 0,
              })}
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
                  { value: "jsonl", label: "JSONL" },
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
              const srcLabel = vault.name || encode(vault.id);
              if (!confirm(`Migrate "${srcLabel}" to "${trimmedName}"? This will immediately disable "${srcLabel}" and delete it after all records are moved.`)) return;
              const destType = migrateTarget.type || undefined;
              const params: Record<string, string> = {};
              if (migrateTarget.dir.trim()) {
                params["dir"] = migrateTarget.dir.trim();
              }
              const destParams = Object.keys(params).length > 0 ? params : undefined;
              try {
                const result = await migrate.mutateAsync({
                  source: encode(vault.id),
                  destination: trimmedName,
                  destinationType: destType,
                  destinationParams: destParams,
                });
                setActiveJob({ jobId: encode(result.jobId), label: "Migrating" });
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
              const destName = vaults.find((s) => encode(s.id) === mergeTarget)?.name || mergeTarget;
              if (!confirm(`Merge "${vault.name || encode(vault.id)}" into "${destName}"? This will immediately disable "${vault.name || encode(vault.id)}" and delete it after all records are moved.`)) return;
              try {
                const result = await merge.mutateAsync({
                  source: encode(vault.id),
                  destination: mergeTarget,
                });
                setActiveJob({ jobId: encode(result.jobId), label: "Merging" });
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
