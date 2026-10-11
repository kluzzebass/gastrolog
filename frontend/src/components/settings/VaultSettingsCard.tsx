import { encode, decode } from "../../api/glid";
import { useState } from "react";
import type { RouteConfig, NodeConfig } from "../../api/gen/gastrolog/v1/system_pb";
import type { NodeStorageConfig } from "../../api/gen/gastrolog/v1/storage_pb";
import { VaultType, RetentionRule, VaultConfig } from "../../api/gen/gastrolog/v1/system_pb";
import {
  usePutVault,
  useDeleteVault,
  useSealVault,
  useReindexVault,
  useRetryUnreadableChunks,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { HelpButton } from "../HelpButton";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput } from "./FormField";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { PulseIcon } from "../icons";
import { CrossLinkBadge } from "../inspector/CrossLinkBadge";
import { JobProgress } from "./VaultHelpers";
import { useThemeClass } from "../../hooks/useThemeClass";
import { leaderNodeId, followerNodeIds } from "../../utils/placement";
import { buildNodeNameMap, resolveNodeName } from "../../utils/nodeNames";

import { VaultStorageForm } from "./VaultsSettings";
import {
  isCloudBacked,
  isStorageComplete,
  storageIncompleteReason,
  vaultTypeEnum,
  type StorageEntry,
  type VaultTypeLabel,
} from "./vaultStorageEntry";
import { transferTargetOptions } from "./transferEligibility";
import { classStorageCounts, placementStatus, plural, rfCap } from "./vaultPlacement";

const DELETING_VAULT_HELP_REF = "vaults-config#deleting-a-vault";

/**
 * Why Delete is refused, or undefined: the server refuses while any route
 * names the vault as a destination.
 */
function routeReferenceReason(vaultId: string, routes: readonly RouteConfig[]): string | undefined {
  const names = routes
    .filter((r) => r.destinations.some((d) => encode(d.vaultId) === vaultId))
    .map((r) => r.name || encode(r.id));
  if (names.length === 0) return undefined;
  const noun = names.length === 1 ? "route" : "routes";
  return `Destination of ${noun} ${names.join(", ")} — remove it there first`;
}

/** What a confirmed delete does, one line each. */
function vaultDeleteConsequences(vault: VaultConfig, holderNames: string[], deleteData: boolean): string[] {
  const name = vault.name || encode(vault.id);
  const lines = [
    holderNames.length > 0
      ? `Removes "${name}" and its ${String(holderNames.length)} placement(s) on ${holderNames.join(", ")}; nothing routes to it afterwards`
      : `Removes "${name}", which has no placements`,
  ];
  switch (vault.type) {
    case VaultType.MEMORY:
      lines.push("Its records live only in memory and are gone with it");
      break;
    case VaultType.JSONL:
      lines.push("The JSONL file stays on disk");
      break;
    default:
      lines.push(deleteData
        ? "Its chunk files are erased from every placement node"
        : "Its chunk files stay on disk, unreachable, until removed by hand");
  }
  return lines;
}

interface VaultSettingsCardProps {
  vault: VaultConfig;
  vaults: VaultConfig[];
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

// The vault carries its full storage shape directly: Type, RotationPolicyID,
// RetentionRules, RF, StorageClass, CloudServiceID, cache fields. The UI
// edits the vault directly; inter-vault routing covers retention chains.

function vaultTypeLabel(t: VaultType): VaultTypeLabel {
  switch (t) {
    case VaultType.MEMORY: return "memory";
    case VaultType.JSONL:  return "jsonl";
    default:               return "file";
  }
}

// vaultToEntry projects the vault's storage shape onto the StorageEntry
// form shape so VaultStorageForm can edit it. The `key` field is the
// vault ID.
function vaultToEntry(v: VaultConfig): StorageEntry {
  return {
    key: encode(v.id),
    type: vaultTypeLabel(v.type),
    storageClass: String(v.storageClass || 0),
    cloudServiceId: v.cloudServiceId.length > 0 ? encode(v.cloudServiceId) : "",
    cacheEviction: v.cacheEviction || "lru",
    cacheBudget: v.cacheBudget,
    cacheTTL: v.cacheTtl,
    memoryBudget: v.memoryBudget,
    rotationPolicyId: v.rotationPolicyId.length > 0 ? encode(v.rotationPolicyId) : "",
    retentionPolicyIds: v.retentionRules.map((r) => encode(r.retentionPolicyId)),
    retentionDisposition: v.retentionDisposition || "delete",
    retentionTransferTarget: v.retentionTransferTargetVaultId.length > 0 ? encode(v.retentionTransferTargetVaultId) : "",
    replicationFactor: String(v.replicationFactor || 1),
    path: v.path || "",
    nodeId: "",
  };
}

// entryToVault rolls a StorageEntry edit back into a VaultConfig (caller
// supplies name and enabled).
//
// Placements are deliberately NOT sent. They are owned by the placement manager
// and PutVault ignores them, re-deriving from the owner, so echoing them back
// would be a no-op that reads like intent.
function entryToVault(
  vault: VaultConfig,
  entry: StorageEntry,
  name: string,
  enabled: boolean,
): VaultConfig {
  const cloudBacked = isCloudBacked(entry);
  return new VaultConfig({
    id: vault.id,
    name,
    enabled,
    type: vaultTypeEnum(entry.type),
    storageClass: entry.type === "file" ? parseInt(entry.storageClass, 10) || 0 : 0,
    cloudServiceId: cloudBacked ? decode(entry.cloudServiceId) : new Uint8Array(0),
    cacheEviction: cloudBacked ? (entry.cacheEviction || "lru") : "",
    // Empty field = unset (server defaults it for cloud vaults), not explicit
    // 0 (rejected) — same size-expression convention as the storage's
    // disk-free thresholds.
    cacheBudget: cloudBacked ? entry.cacheBudget : "",
    cacheTtl: cloudBacked ? entry.cacheTTL : "",
    // Empty field = unset (server defaults it for memory vaults), not
    // explicit 0 (rejected).
    memoryBudget: entry.type === "memory" ? entry.memoryBudget : "",
    rotationPolicyId: entry.rotationPolicyId ? decode(entry.rotationPolicyId) : new Uint8Array(0),
    retentionRules: entry.retentionPolicyIds.map((id) => new RetentionRule({ retentionPolicyId: decode(id) })),
    retentionDisposition: entry.type !== "jsonl" ? (entry.retentionDisposition || "delete") : "",
    retentionTransferTargetVaultId:
      entry.type !== "jsonl" && entry.retentionDisposition === "transfer" && entry.retentionTransferTarget
        ? decode(entry.retentionTransferTarget)
        : new Uint8Array(0),
    replicationFactor: entry.type === "jsonl" ? 1 : parseInt(entry.replicationFactor, 10) || 1,
    path: entry.type === "jsonl" ? entry.path : "",
  });
}

export function VaultSettingsCard({
  vault,
  vaults,
  routes,
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
  const deleteVault = useDeleteVault();
  const seal = useSealVault();
  const reindex = useReindexVault();
  const retryUnreadable = useRetryUnreadableChunks();
  const { addToast } = useToast();

  const [deleteData, setDeleteData] = useState(false);
  const [activeJob, setActiveJob] = useState<{ jobId: string; label: string } | null>(null);

  const classStorageCount = classStorageCounts(nodeStorageConfigs);
  const totalNodes = nodeConfigs.length;

  // Edit state — single StorageEntry projection of the vault's storage
  // shape, plus name+enabled.
  interface VaultEdit {
    name: string;
    enabled: boolean;
    storage: StorageEntry;
  }
  const buildInitialEdit = (): VaultEdit => ({
    name: vault.name,
    enabled: vault.enabled,
    storage: vaultToEntry(vault),
  });
  const [edit, setEditState] = useState<VaultEdit>(buildInitialEdit);
  const resetEdit = () => setEditState(buildInitialEdit());
  const setEdit = (patch: Partial<VaultEdit>) => setEditState((prev) => ({ ...prev, ...patch }));
  const updateStorage = (patch: Partial<StorageEntry>) =>
    setEditState((prev) => ({ ...prev, storage: { ...prev.storage, ...patch } }));

  // Re-sync the edit form when the vault prop changes (e.g. after a save
  // refreshes the config cache, or after a rotate/reindex action that
  // doesn't touch the vault config but might surface elsewhere).
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

  const anyDirty = JSON.stringify(edit) !== initialJson;
  const storageComplete = isStorageComplete(edit.storage, cloudServiceOptions.length > 0);

  const { handleSave: saveVault, handleDelete } = useCrudHandlers({
    mutation: putVault,
    deleteMutation: deleteVault,
    label: "Vault",
    onSaveTransform: (_id, e: VaultEdit) => ({
      config: entryToVault(vault, e.storage, e.name, e.enabled),
    }),
    onDeleteTransform: (id) => ({ id, force: true, deleteData }),
  });

  const handleSaveAll = async () => {
    await saveVault(encode(vault.id), edit);
    setPendingReset(true);
  };

  // Placement is read off the vault's own placements (owned by the
  // placement manager) and the cluster's storage configs.
  const nodeNameMap = buildNodeNameMap(nodeConfigs);
  const leaderId = leaderNodeId(vault, nodeStorageConfigs);
  const leaderName = leaderId ? resolveNodeName(nodeNameMap, leaderId) : null;
  const followerNames = followerNodeIds(vault, nodeStorageConfigs).map((id) => resolveNodeName(nodeNameMap, id));
  const csName = (vault.cloudServiceId.length > 0
    ? cloudServiceOptions.find((cs) => cs.value === encode(vault.cloudServiceId))?.label
    : null) ?? null;
  const savedShape = { type: vaultTypeLabel(vault.type), storageClass: String(vault.storageClass || 0) };
  const placement = placementStatus(vault, savedShape, classStorageCount, totalNodes);
  const shortfall = placement.action
    ? `${String(placement.placed)} of ${plural(placement.rf, "placement")} — ${placement.action}`
    : null;
  const labelClass = `text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`;
  const mutedClass = `text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`;
  const holderNames = leaderName ? [leaderName, ...followerNames] : followerNames;
  const deleteDisabledReason = routeReferenceReason(encode(vault.id), routes);

  return (
    <SettingsCard
      key={encode(vault.id)}
      id={vault.name || encode(vault.id)}
      typeBadge={vaultTypeLabel(vault.type)}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={() => handleDelete(encode(vault.id))}
      deleteLabel="Delete"
      deleteDisabledReason={deleteDisabledReason}
      deleteConsequences={vaultDeleteConsequences(vault, holderNames, deleteData)}
      deleteConfirmExtra={
        <div className="flex items-center gap-1.5">
          {vault.type === VaultType.FILE && (
            <label className="flex items-center gap-1.5 text-[0.8em] opacity-70">
              <input
                type="checkbox"
                checked={deleteData}
                onChange={(e) => setDeleteData(e.target.checked)}
              />
              Delete data files
            </label>
          )}
          <HelpButton helpRef={DELETING_VAULT_HELP_REF} />
        </div>
      }
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
          <HelpButton helpRef="vaults-config#editing-a-vault" />
          {anyDirty && (
            <Button
              variant="ghost"
              bordered
              dark={dark}
              onClick={resetEdit}
            >
              Discard
            </Button>
          )}
          <Button
            onClick={handleSaveAll}
            disabled={putVault.isPending || !anyDirty || !storageComplete}
            disabledReason={storageIncompleteReason(edit.storage)}
          >
            {putVault.isPending ? "Saving..." : "Save"}
          </Button>
        </>
      }
      headerRight={
        <span className="flex items-center gap-2">
          {!vault.enabled && (
            <Badge variant="muted" dark={dark}>disabled</Badge>
          )}
          {csName && (
            <Badge
              variant="muted"
              dark={dark}
              title={`cloud-backed — sealed chunks upload to cloud service ${csName}`}
              helpRef="storage-cloud"
            >
              {`cloud-backed: ${csName}`}
            </Badge>
          )}
          {shortfall && (
            <span className="text-[0.85em] text-severity-error">{shortfall}</span>
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

        {/* Placement — the saved shape's leader, followers, RF cap and
            cause, read-only; the form below edits the shape itself. */}
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-1.5">
            <span className={labelClass}>Placement</span>
            <HelpButton helpRef="vaults-config#placement" />
          </div>
          <div className={`flex flex-col gap-0.5 ${mutedClass}`}>
            {placement.unplacedCause ? (
              <span>{`unplaced — ${placement.unplacedCause}`}</span>
            ) : (
              <span>
                {`leader: ${leaderName ?? ""}`}
                {followerNames.length > 0 && ` · followers: ${followerNames.join(", ")}`}
              </span>
            )}
            {vault.type === VaultType.JSONL ? (
              <span className="font-mono">{vault.path || `jsonl/${vault.name || "vault"}.jsonl`}</span>
            ) : (
              <span>{`RF ${String(placement.rf)} · cap ${String(placement.cap.max)} (${placement.cap.cause})`}</span>
            )}
            {vault.type === VaultType.MEMORY && vault.memoryBudget !== "" && (
              <span className="font-mono">{vault.memoryBudget}</span>
            )}
            {placement.action && (
              <span className="text-severity-error">
                {`short by ${plural(placement.rf - placement.placed, "placement")} — ${placement.action}`}
              </span>
            )}
          </div>
        </div>

        <div className="flex flex-col gap-1.5">
          <span className={labelClass}>Actions</span>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              bordered
              dark={dark}
              disabled={seal.isPending || !!activeJob}
              onClick={async () => {
                try {
                  const sealed = await seal.mutateAsync(encode(vault.id));
                  addToast(
                    sealed > 0
                      ? `Sealed ${String(sealed)} open chunk(s)`
                      : "Nothing to rotate: no open chunk holds records",
                    "info",
                  );
                } catch (err: unknown) {
                  addToast(err instanceof Error ? err.message : "Rotate failed", "error");
                }
              }}
            >
              {seal.isPending ? "Rotating..." : "Rotate"}
            </Button>
            <span className={mutedClass}>Seals every active chunk holding records; the next record opens a new one.</span>
            <HelpButton helpRef="policy-rotation#rotating-now" />
          </div>
          <div className="flex items-center gap-2">
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
            <span className={mutedClass}>Rebuilds every sealed chunk's indexes as a tracked job; records are untouched.</span>
            <HelpButton helpRef="vaults-config#actions" />
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              bordered
              dark={dark}
              disabled={retryUnreadable.isPending || !!activeJob}
              onClick={async () => {
                try {
                  const result = await retryUnreadable.mutateAsync(encode(vault.id));
                  if (result.retriedCount === 0) {
                    addToast("No unreadable chunks to retry", "info");
                  } else {
                    addToast(`Reset backoff on ${String(result.retriedCount)} chunk(s); next retention sweep will retry`, "info");
                  }
                } catch (err: unknown) {
                  addToast(err instanceof Error ? err.message : "Retry unreadable failed", "error");
                }
              }}
            >
              {retryUnreadable.isPending ? "Retrying..." : "Retry Unreadable"}
            </Button>
            <span className={mutedClass}>Resets the retry backoff on unreadable chunks so the next retention sweep retries them all.</span>
            <HelpButton helpRef="vaults-config#actions" />
          </div>
        </div>

        {/* Storage shape edit — same form the Add flow uses, with type
            and cloud binding locked because the backend rejects those
            mutations on existing vaults. The Storage Type selector is
            hidden entirely (no onTypeChange passed);
            the Cloud Storage selector is disabled via cloudLocked. To
            change type or cloud binding, create a new vault and migrate
            via retention routing. */}
        <VaultStorageForm
          storage={edit.storage}
          dark={dark}
          storageClassOptions={storageClassOptions}
          cloudServiceOptions={cloudServiceOptions}
          rotationPolicyOptions={rotationPolicyOptions}
          retentionPolicyOptions={retentionPolicyOptions}
          transferTargetOptions={transferTargetOptions(vaults, encode(vault.id))}
          nodeOptions={nodeConfigs.map((n) => ({ value: encode(n.id), label: n.name || encode(n.id) })).sort((a, b) => a.label.localeCompare(b.label))}
          vaultName={vault.name || ""}
          cap={rfCap(edit.storage, classStorageCount, totalNodes)}
          cloudLocked
          onUpdate={updateStorage}
        />
      </div>
    </SettingsCard>
  );
}
