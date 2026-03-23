import { useState } from "react";
import { protoInt64 } from "@bufbuild/protobuf";
import type { VaultConfig, RouteConfig, NodeConfig } from "../../api/gen/gastrolog/v1/config_pb";
import type { NodeStorageConfig } from "../../api/gen/gastrolog/v1/storage_pb";
import { TierConfig, TierType } from "../../api/gen/gastrolog/v1/config_pb";
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
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput } from "./FormField";
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
    case TierType.LOCAL: return "local";
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
  nodeStorageConfigs: _nodeStorageConfigs,
  storageClassOptions,
  cloudServiceOptions,
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
  const [creatingTier, setCreatingTier] = useState(false);

  // Per-vault state — previously Record maps in the parent.
  const [deleteData, setDeleteData] = useState(false);
  const [migrateTarget, setMigrateTarget] = useState<{ name: string; type: string; dir: string } | null>(null);
  const [mergeTarget, setMergeTarget] = useState<string | null>(null);
  const [activeJob, setActiveJob] = useState<{ jobId: string; label: string } | null>(null);

  const defaults = (_id: string) => ({
    name: vault.name,
    enabled: vault.enabled,
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(vault.id);

  // Tier IDs managed separately — useEditState discards edits on config push,
  // which breaks reordering because the placement manager writes PutTier.
  const [localTierIds, setLocalTierIds] = useState<string[]>([...vault.tierIds]);
  const tierIdsDirty = JSON.stringify(localTierIds) !== JSON.stringify([...vault.tierIds]);

  // Resolve vault's tiers from local state (reflects unsaved reorder/remove).
  const tierMap = new Map(tiers.map((t) => [t.id, t]));
  const vaultTiers = localTierIds.map((id) => tierMap.get(id)).filter((t): t is TierConfig => !!t);

  // Node name resolution for tier placement display.
  const nodeNameMap = new Map(nodeConfigs.map((n) => [n.id, n.name || n.id]));
  const resolveNodeName = (nodeId: string) => nodeNameMap.get(nodeId) || nodeId;

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
    clearEdit,
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
      deleteConfirmExtra={vaultTiers.some((t) => t.type === TierType.LOCAL) ? (
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
          {(isDirty(vault.id) || tierIdsDirty) && (
            <Button
              variant="ghost"
              bordered
              dark={dark}
              onClick={() => { clearEdit(vault.id); setLocalTierIds([...vault.tierIds]); setNewTier(null); }}
            >
              Cancel
            </Button>
          )}
          <Button
            onClick={() => saveVault(vault.id, { ...edit, tierIds: localTierIds })}
            disabled={putVault.isPending || (!isDirty(vault.id) && !tierIdsDirty)}
          >
            {putVault.isPending ? "Saving..." : "Save"}
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
            onChange={(v) => setEdit(vault.id, { name: v })}
            dark={dark}
          />
        </FormField>
        <Checkbox
          checked={edit.enabled}
          onChange={(v) => setEdit(vault.id, { enabled: v })}
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
                      {tier.type === TierType.LOCAL && tier.storageClass > 0 && (
                        <span className="font-mono">{`class ${String(tier.storageClass)}`}</span>
                      )}
                      {tier.type === TierType.MEMORY && tier.memoryBudgetBytes > 0 && (
                        <span className="font-mono">{formatBytes(tier.memoryBudgetBytes)}</span>
                      )}
                      {tier.type === TierType.CLOUD && csName && <span>{csName}</span>}
                      {tier.type === TierType.CLOUD && tier.activeChunkClass > 0 && (
                        <span className="font-mono">{`chunk class ${String(tier.activeChunkClass)}`}</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          {newTier ? (
            <div className="flex flex-col gap-2">
              <TierEntryCard
                tier={newTier}
                index={vaultTiers.length}
                dark={dark}
                storageClassOptions={storageClassOptions}
                cloudServiceOptions={cloudServiceOptions}
                onUpdate={(patch) => setNewTier((t) => t ? { ...t, ...patch } : t)}
                onRemove={() => setNewTier(null)}
              />
              <div className="flex justify-end gap-2">
                <Button onClick={() => setNewTier(null)}>Cancel</Button>
                <Button
                  onClick={async () => {
                    if (!isTierComplete(newTier, cloudServiceOptions.length > 0)) return;
                    const tierId = crypto.randomUUID();
                    const tierCfg = new TierConfig({
                      id: tierId,
                      name: newTier.type,
                      type: tierTypeEnum(newTier.type),
                      storageClass: newTier.type === "local" ? parseInt(newTier.storageClass, 10) || 0 : 0,
                      cloudServiceId: newTier.type === "cloud" ? newTier.cloudServiceId : "",
                      activeChunkClass: newTier.type === "cloud" ? parseInt(newTier.activeChunkClass, 10) || 0 : 0,
                      cacheClass: newTier.type === "cloud" ? parseInt(newTier.cacheClass, 10) || 0 : 0,
                      memoryBudgetBytes: newTier.type === "memory" ? parseMemoryBudget(newTier.memoryBudget) : protoInt64.zero,
                    });
                    setCreatingTier(true);
                    try {
                      await putTier.mutateAsync({ config: tierCfg });
                      setLocalTierIds([...localTierIds, tierId]);
                      setNewTier(null);
                      setCreatingTier(false);
                      addToast("Tier created — save to apply", "info");
                    } catch (err: unknown) {
                      setCreatingTier(false);
                      const msg = err instanceof Error ? err.message : "Failed to add tier";
                      addToast(msg, "error");
                    }
                  }}
                  disabled={creatingTier || !isTierComplete(newTier, cloudServiceOptions.length > 0)}
                >
                  {creatingTier ? "Creating..." : "Create Tier"}
                </Button>
              </div>
            </div>
          ) : (
            <DropdownButton
              label="+ Add Tier"
              items={[
                { value: "memory", label: "Memory" },
                { value: "local", label: "Local" },
                { value: "cloud", label: "Cloud" },
              ]}
              onSelect={(v) => setNewTier(emptyTierEntry(v as TierTypeLabel))}
              dark={dark}
            />
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
