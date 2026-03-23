import { useState } from "react";
import type { VaultConfig, TierConfig, RouteConfig } from "../../api/gen/gastrolog/v1/config_pb";
import { TierType } from "../../api/gen/gastrolog/v1/config_pb";
import {
  usePutVault,
  useDeleteVault,
  useSealVault,
  useReindexVault,
  useMigrateVault,
  useMergeVaults,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput } from "./FormField";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { PulseIcon } from "../icons";
import { CrossLinkBadge } from "../inspector/CrossLinkBadge";
import { JobProgress } from "./VaultHelpers";
import { MigrateVaultForm, MergeVaultForm } from "./VaultMigrateForms";
import { useThemeClass } from "../../hooks/useThemeClass";

interface VaultSettingsCardProps {
  vault: VaultConfig;
  vaults: VaultConfig[];
  tiers: TierConfig[];
  routes: RouteConfig[];
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onOpenInspector?: (inspectorParam: string) => void;
}

export function VaultSettingsCard({
  vault,
  vaults,
  tiers,
  routes,
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
  const migrate = useMigrateVault();
  const merge = useMergeVaults();
  const { addToast } = useToast();

  // Per-vault state — previously Record maps in the parent.
  const [deleteData, setDeleteData] = useState(false);
  const [migrateTarget, setMigrateTarget] = useState<{ name: string; type: string; dir: string } | null>(null);
  const [mergeTarget, setMergeTarget] = useState<string | null>(null);
  const [activeJob, setActiveJob] = useState<{ jobId: string; label: string } | null>(null);

  // Resolve vault's tiers from the tier list.
  const tierMap = new Map(tiers.map((t) => [t.id, t]));
  const vaultTiers = vault.tierIds.map((id) => tierMap.get(id)).filter((t): t is TierConfig => !!t);

  const tierTypeLabel = (type: TierType): string => {
    switch (type) {
      case TierType.MEMORY: return "memory";
      case TierType.LOCAL: return "local";
      case TierType.CLOUD: return "cloud";
      default: return "unknown";
    }
  };

  const defaults = (_id: string) => ({
    name: vault.name,
    tierIds: [...vault.tierIds],
    enabled: vault.enabled,
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(vault.id);

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
          <Button
            onClick={() => saveVault(vault.id, edit)}
            disabled={putVault.isPending || !isDirty(vault.id)}
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
        {/* Tier list (read-only summary; tier editing is a separate issue) */}
        <div className="flex flex-col gap-1.5">
          <span className={`text-[0.75em] font-medium uppercase tracking-[0.12em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            Tiers
          </span>
          {vaultTiers.length === 0 ? (
            <span className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
              No tiers assigned.
            </span>
          ) : (
            <div className="flex flex-wrap gap-1.5">
              {vaultTiers.map((tier) => (
                <Badge key={tier.id} variant="muted" dark={dark}>
                  {tier.name || tier.id} ({tierTypeLabel(tier.type)})
                </Badge>
              ))}
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
