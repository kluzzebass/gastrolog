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

import {
  VaultStorageForm,
  isCloudBacked,
  isStorageComplete,
  vaultTypeEnum,
  type StorageEntry,
  type VaultTypeLabel,
} from "./VaultsSettings";

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
    retentionPolicyId: v.retentionRules[0] ? encode(v.retentionRules[0].retentionPolicyId) : "",
    retentionDisposition: v.retentionDisposition || "delete",
    diskFreeWarn: v.diskFreeWarn,
    diskFreeFloor: v.diskFreeFloor,
    replicationFactor: String(v.replicationFactor || 1),
    path: v.path || "",
    nodeId: "",
  };
}

// entryToVault rolls a StorageEntry edit back into a VaultConfig, preserving
// fields that aren't part of the form (placements — caller supplies name and
// enabled).
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
    // 0 (rejected); same size-expression convention as diskFreeFloor below
    // (gastrolog-338j51).
    cacheBudget: cloudBacked ? entry.cacheBudget : "",
    cacheTtl: cloudBacked ? entry.cacheTTL : "",
    // Empty field = unset (server defaults it for memory vaults), not
    // explicit 0 (rejected) (gastrolog-1qd5wz).
    memoryBudget: entry.type === "memory" ? entry.memoryBudget : "",
    rotationPolicyId: entry.rotationPolicyId ? decode(entry.rotationPolicyId) : new Uint8Array(0),
    retentionRules: entry.retentionPolicyId
      ? [new RetentionRule({ retentionPolicyId: decode(entry.retentionPolicyId) })]
      : [],
    retentionDisposition: entry.type !== "jsonl" ? (entry.retentionDisposition || "delete") : "",
    diskFreeWarn: entry.type === "file" ? entry.diskFreeWarn : "",
    diskFreeFloor: entry.type === "file" ? entry.diskFreeFloor : "",
    replicationFactor: entry.type === "jsonl" ? 1 : parseInt(entry.replicationFactor, 10) || 1,
    path: entry.type === "jsonl" ? entry.path : "",
    placements: vault.placements,
  });
}

export function VaultSettingsCard({
  vault,
  vaults: _vaults,
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
  const deleteVault = useDeleteVault();
  const seal = useSealVault();
  const reindex = useReindexVault();
  const retryUnreadable = useRetryUnreadableChunks();
  const { addToast } = useToast();

  const [deleteData, setDeleteData] = useState(false);
  const [activeJob, setActiveJob] = useState<{ jobId: string; label: string } | null>(null);

  // RF cap derives from how many file storages exist for the chosen
  // storage class. Memory caps at total node count; JSONL is a single-
  // node sink.
  const classStorageCount = new Map<number, number>();
  for (const nsc of nodeStorageConfigs) {
    for (const fs of nsc.fileStorages) {
      classStorageCount.set(fs.storageClass, (classStorageCount.get(fs.storageClass) ?? 0) + 1);
    }
  }
  const totalNodes = nodeConfigs.length || 1;
  const maxRFForEntry = (e: StorageEntry) => {
    if (e.type === "memory") return totalNodes;
    if (e.type === "jsonl") return 1;
    const sc = parseInt(e.storageClass, 10) || 0;
    if (sc === 0) return 1;
    return classStorageCount.get(sc) ?? 1;
  };

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

  // Placement summary for the header — node where the leader storage
  // lives, plus follower nodes for cluster vaults.
  const nodeNameMap = buildNodeNameMap(nodeConfigs);
  const leaderId = leaderNodeId(vault, nodeStorageConfigs);
  const leaderName = leaderId ? resolveNodeName(nodeNameMap, leaderId) : null;
  const followerIds = followerNodeIds(vault, nodeStorageConfigs);
  const csName = (vault.cloudServiceId.length > 0
    ? cloudServiceOptions.find((cs) => cs.value === encode(vault.cloudServiceId))?.label
    : null) ?? null;
  const rfActual = (vault.replicationFactor || 1);
  const rfShortfall = rfActual > 1 && followerIds.length + 1 < rfActual;

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
      deleteConfirmExtra={vault.type === VaultType.FILE ? (
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
            <Badge variant="muted" dark={dark} title="Cloud-backed">{csName}</Badge>
          )}
          {rfShortfall && (
            <span className="text-[0.85em] text-severity-error">
              {`insufficient nodes for RF=${String(rfActual)}`}
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

        {/* Placement summary — leader + followers + RF. The vault's
            storage shape (rotation, retention, RF, cache fields) is
            edited below in the storage form. */}
        <div className={`flex flex-wrap items-center gap-3 text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
          {leaderName ? (
            <span>{`leader: ${leaderName}`}</span>
          ) : (
            <span>unplaced</span>
          )}
          {vault.type === VaultType.FILE && vault.storageClass > 0 && (
            <span className="font-mono">{`class ${String(vault.storageClass)}`}</span>
          )}
          {vault.type === VaultType.JSONL && (
            <span className="font-mono">
              {vault.path || `jsonl/${vault.name || "vault"}.jsonl`}
            </span>
          )}
          {vault.type === VaultType.MEMORY && vault.memoryBudget !== "" && (
            <span className="font-mono">{vault.memoryBudget}</span>
          )}
          {vault.type !== VaultType.JSONL && (
            <span>{`RF=${String(rfActual)}`}</span>
          )}
          {followerIds.length > 0 && (
            <span>
              {"followers: "}
              {followerIds.map((id, si) => {
                const name = resolveNodeName(nodeNameMap, id);
                return (
                  <span key={id}>
                    {si > 0 && ", "}
                    {name}
                  </span>
                );
              })}
            </span>
          )}
        </div>

        {/* Storage shape edit — same form the Add flow uses, with type
            and cloud binding locked because the backend rejects those
            mutations on existing vaults (gastrolog-3ul0s). The Storage
            Type selector is hidden entirely (no onTypeChange passed);
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
          nodeOptions={nodeConfigs.map((n) => ({ value: encode(n.id), label: n.name || encode(n.id) })).sort((a, b) => a.label.localeCompare(b.label))}
          vaultName={vault.name || ""}
          maxRF={maxRFForEntry(edit.storage)}
          cloudLocked
          onUpdate={updateStorage}
        />
      </div>
    </SettingsCard>
  );
}
