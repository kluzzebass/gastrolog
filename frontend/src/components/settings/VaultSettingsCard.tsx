import { useState } from "react";
import type { VaultConfig, RotationPolicyConfig, RetentionPolicyConfig, RouteConfig } from "../../api/gen/gastrolog/v1/config_pb";
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
import { FormField, TextInput, SelectInput } from "./FormField";
import { VaultParamsForm } from "./VaultParamsForm";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { NodeBadge } from "./NodeBadge";
import { NodeSelect } from "./NodeSelect";
import { PulseIcon } from "../icons";
import { CrossLinkBadge } from "../inspector/CrossLinkBadge";
import { JobProgress, RetentionRuleEditor, retentionRulesValid } from "./VaultHelpers";
import type { RetentionRuleEdit } from "./VaultHelpers";
import { MigrateVaultForm, MergeVaultForm } from "./VaultMigrateForms";

interface VaultSettingsCardProps {
  vault: VaultConfig;
  vaults: VaultConfig[];
  routes: RouteConfig[];
  policies: RotationPolicyConfig[];
  retentionPolicies: RetentionPolicyConfig[];
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onOpenInspector?: (inspectorParam: string) => void;
}

export function VaultSettingsCard({
  vault,
  vaults,
  routes,
  policies,
  retentionPolicies,
  dark,
  expanded,
  onToggle,
  onOpenInspector,
}: Readonly<VaultSettingsCardProps>) {
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

  const policyOptions = [
    { value: "", label: "(none)" },
    ...policies
      .map((p) => ({ value: p.id, label: p.name || p.id }))
      .sort((a, b) => a.label.localeCompare(b.label)),
  ];

  const defaults = (_id: string) => ({
    name: vault.name,
    policy: vault.policy,
    retentionRules: vault.retentionRules.map((b): RetentionRuleEdit => ({
      retentionPolicyId: b.retentionPolicyId,
      action: b.action,
      ejectRouteIds: b.ejectRouteIds,
    })),
    enabled: vault.enabled,
    params: { ...vault.params } as Record<string, string>,
    nodeId: vault.nodeId,
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
        policy: string;
        retentionRules: RetentionRuleEdit[];
        enabled: boolean;
        params: Record<string, string>;
        type: string;
        nodeId: string;
      },
    ) => ({
      id,
      name: e.name,
      type: e.type,
      policy: e.policy,
      retentionRules: e.retentionRules,
      params: e.params,
      enabled: e.enabled,
      nodeId: e.nodeId,
    }),
    onDeleteTransform: (id) => ({ id, force: true, deleteData }),
    clearEdit,
  });

  const hasPolicy = vault.policy && policies.some((p) => p.id === vault.policy);
  const hasRetention = vault.retentionRules.length > 0;
  const warnings = [
    ...(!hasPolicy ? ["no rotation policy"] : []),
    ...(!hasRetention ? ["no retention policy"] : []),
  ];

  return (
    <SettingsCard
      key={vault.id}
      id={vault.name || vault.id}
      typeBadge={vault.type}
      secondaryBadge={vault.params["sealed_backing"] || undefined}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={() => handleDelete(vault.id)}
      deleteLabel="Delete"
      deleteConfirmExtra={vault.type === "file" ? (
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
            onClick={() => saveVault(vault.id, { ...edit, type: vault.type })}
            disabled={putVault.isPending || !isDirty(vault.id) || !retentionRulesValid(edit.retentionRules)}
          >
            {putVault.isPending ? "Saving..." : "Save"}
          </Button>
        </>
      }
      headerRight={
        <span className="flex items-center gap-2">
          <NodeBadge nodeId={edit.nodeId} dark={dark} />
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
        <NodeSelect
          value={edit.nodeId}
          onChange={(v) => setEdit(vault.id, { nodeId: v })}
          dark={dark}
        />
        <FormField label="Rotation Policy" dark={dark}>
          <SelectInput
            value={edit.policy}
            onChange={(v) => setEdit(vault.id, { policy: v })}
            options={policyOptions}
            dark={dark}
          />
        </FormField>
        <RetentionRuleEditor
          rules={edit.retentionRules}
          onChange={(rules) => setEdit(vault.id, { retentionRules: rules })}
          retentionPolicies={retentionPolicies}
          routes={routes}
          dark={dark}
        />
        <VaultParamsForm
          vaultType={vault.type}
          params={edit.params}
          onChange={(p) => setEdit(vault.id, { params: p })}
          dark={dark}
          vaultName={edit.name || vault.name}
        />
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
