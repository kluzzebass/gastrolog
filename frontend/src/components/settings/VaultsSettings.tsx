import { useState } from "react";
import { useExpandedCard } from "../../hooks/useExpandedCards";
import {
  useConfig,
  usePutVault,
  useDeleteVault,
  useSealVault,
  useReindexVault,
  useMigrateVault,
  useMergeVaults,
  useGenerateName,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { Badge } from "../Badge";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { VaultParamsForm } from "./VaultParamsForm";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { NodeBadge } from "./NodeBadge";
import { NodeSelect } from "./NodeSelect";
import { sortByName } from "../../lib/sort";
import { JobProgress, RetentionRulesEditor, retentionRulesValid } from "./VaultHelpers";
import type { RetentionRuleEdit } from "./VaultHelpers";
import { MigrateVaultForm, MergeVaultForm } from "./VaultMigrateForms";

export function VaultsSettings({ dark, expandTarget, onExpandTargetConsumed }: Readonly<{ dark: boolean; expandTarget?: string | null; onExpandTargetConsumed?: () => void }>) {
  const { data: config, isLoading } = useConfig();
  const putVault = usePutVault();
  const deleteVault = useDeleteVault();
  const seal = useSealVault();
  const reindex = useReindexVault();
  const migrate = useMigrateVault();
  const merge = useMergeVaults();
  const { addToast } = useToast();

  const { isExpanded, toggle: toggleCard, setExpanded } = useExpandedCard();
  const [adding, setAdding] = useState(false);
  const [deleteDataFlags, setDeleteDataFlags] = useState<Record<string, boolean>>({});
  const [migrateTarget, setMigrateTarget] = useState<
    Record<string, { name: string; type: string; dir: string }>
  >({});
  const [mergeTarget, setMergeTarget] = useState<Record<string, string>>({});
  // Track active jobs per vault: { vaultId: { jobId, label } }
  const [activeJobs, setActiveJobs] = useState<
    Record<string, { jobId: string; label: string }>
  >({});

  const generateName = useGenerateName();

  // New vault form state.
  const [newName, setNewName] = useState("");
  const [namePlaceholder, setNamePlaceholder] = useState("");
  const [newType, setNewType] = useState("memory");
  const [newPolicy, setNewPolicy] = useState("");
  const [newRetentionRules, setNewRetentionRules] = useState<RetentionRuleEdit[]>([]);
  const [newParams, setNewParams] = useState<Record<string, string>>({});
  const [newNodeId, setNewNodeId] = useState("");

  const configVaults = config?.vaults;
  const vaults = configVaults ?? [];
  const existingNames = new Set(vaults.map((s) => s.name));
  const effectiveName = newName.trim() || namePlaceholder || newType;
  const nameConflict = existingNames.has(effectiveName);
  const policies = config?.rotationPolicies ?? [];
  const retentionPolicies = config?.retentionPolicies ?? [];

  // Auto-expand a vault when navigated to from another settings tab.
  // React-recommended "adjusting state when a prop changes" pattern.
  const [prevExpandTarget, setPrevExpandTarget] = useState(expandTarget);
  if (expandTarget && expandTarget !== prevExpandTarget && configVaults && configVaults.length > 0) {
    setPrevExpandTarget(expandTarget);
    const match = configVaults.find((s) => (s.name || s.id) === expandTarget);
    if (match) {
      setExpanded(match.id);
    }
    onExpandTargetConsumed?.();
  }

  const policyOptions = [
    { value: "", label: "(none)" },
    ...policies.map((p) => ({ value: p.id, label: p.name || p.id })),
  ];

  const defaults = (id: string) => {
    const vault = vaults.find((s) => s.id === id);
    if (!vault)
      return {
        name: "",
        policy: "",
        retentionRules: [] as RetentionRuleEdit[],
        enabled: true,
        params: {} as Record<string, string>,
        nodeId: "",
      };
    return {
      name: vault.name,
      policy: vault.policy,
      retentionRules: vault.retentionRules.map((b) => ({
        retentionPolicyId: b.retentionPolicyId,
        action: b.action,
        destinationId: b.destinationId,
      })),
      enabled: vault.enabled,
      params: { ...vault.params },
      nodeId: vault.nodeId,
    };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveVault, handleDelete } = useCrudHandlers({
    mutation: putVault,
    deleteMutation: deleteVault,
    label: "Vault",
    onSaveTransform: (
      id,
      edit: {
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
      name: edit.name,
      type: edit.type,
      policy: edit.policy,
      retentionRules: edit.retentionRules,
      params: edit.params,
      enabled: edit.enabled,
      nodeId: edit.nodeId,
    }),
    onDeleteTransform: (id) => ({ id, force: true, deleteData: deleteDataFlags[id] ?? false }),
    clearEdit,
  });

  const clearJob = (vaultId: string) => {
    setActiveJobs((prev) => {
      const next = { ...prev };
      delete next[vaultId];
      return next;
    });
  };

  const handleCreate = async () => {
    const name = newName.trim() || namePlaceholder || newType;
    try {
      await putVault.mutateAsync({
        id: "",
        name,
        type: newType,
        policy: newPolicy,
        retentionRules: newRetentionRules,
        params: newParams,
        nodeId: newNodeId,
      });
      addToast(`Vault "${name}" created`, "info");
      setAdding(false);
      setNewName("");
      setNewType("memory");
      setNewPolicy("");
      setNewRetentionRules([]);
      setNewParams({});
      setNewNodeId("");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create vault", "error");
    }
  };

  return (
    <SettingsSection
      addLabel="Add Vault"
      adding={adding}
      onToggleAdd={() => {
        setAdding(false);
        setNewName("");
        setNamePlaceholder("");
        setNewType("memory");
        setNewPolicy("");
        setNewRetentionRules([]);
        setNewParams({});
        setNewNodeId("");
      }}
      addOptions={[
        { value: "memory", label: "memory" },
        { value: "file", label: "file" },
      ]}
      onAddSelect={(type) => {
        generateName.mutateAsync().then(setNamePlaceholder);
        setAdding(true);
        setNewType(type);
        setNewName("");
        setNamePlaceholder("");
        setNewPolicy("");
        setNewRetentionRules([]);
        setNewParams({});
        setNewNodeId("");
      }}
      isLoading={isLoading}
      isEmpty={vaults.length === 0}
      emptyMessage='No vaults configured. Click "Add Vault" to create one.'
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => setAdding(false)}
          onCreate={handleCreate}
          isPending={putVault.isPending}
          createDisabled={nameConflict || !retentionRulesValid(newRetentionRules)}
          typeBadge={newType}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={setNewName}
              placeholder={namePlaceholder || newType}
              dark={dark}
            />
          </FormField>
          <NodeSelect value={newNodeId} onChange={setNewNodeId} dark={dark} />
          <FormField label="Rotation Policy" dark={dark}>
            <SelectInput
              value={newPolicy}
              onChange={setNewPolicy}
              options={policyOptions}
              dark={dark}
            />
          </FormField>
          <RetentionRulesEditor
            rules={newRetentionRules}
            onChange={setNewRetentionRules}
            retentionPolicies={retentionPolicies}
            vaults={vaults}
            currentVaultId=""
            dark={dark}
          />
          <VaultParamsForm
            vaultType={newType}
            params={newParams}
            onChange={setNewParams}
            dark={dark}
            vaultName={effectiveName}
          />
        </AddFormCard>
      )}

      {sortByName(vaults).map((vault) => {
        const edit = getEdit(vault.id);
        const hasPolicy = vault.policy && policies.some((p) => p.id === vault.policy);
        const hasRetention = vault.retentionRules.length > 0;
        const warnings = [
          ...(!hasPolicy ? ["no rotation policy"] : []),
          ...(!hasRetention ? ["no retention policy"] : []),
        ];
        const activeJob = activeJobs[vault.id];
        return (
          <SettingsCard
            key={vault.id}
            id={vault.name || vault.id}
            typeBadge={vault.type}
            dark={dark}
            expanded={isExpanded(vault.id)}
            onToggle={() => toggleCard(vault.id)}
            onDelete={() => handleDelete(vault.id)}
            deleteLabel="Delete"
            deleteConfirmExtra={vault.type === "file" ? (
              <label className="flex items-center gap-1.5 text-[0.8em] opacity-70">
                <input
                  type="checkbox"
                  checked={deleteDataFlags[vault.id] ?? false}
                  onChange={(e) => setDeleteDataFlags((prev) => ({ ...prev, [vault.id]: e.target.checked }))}
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
                      clearJob(vault.id);
                    }}
                    onFailed={(job) => {
                      addToast(
                        `${activeJob.label} failed: ${job.error}`,
                        "error",
                      );
                      clearJob(vault.id);
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
                      setActiveJobs((prev) => ({
                        ...prev,
                        [vault.id]: {
                          jobId: result.jobId,
                          label: "Reindexing",
                        },
                      }));
                    } catch (err: unknown) {
                      addToast(err instanceof Error ? err.message : "Reindex failed", "error");
                    }
                  }}
                >
                  {activeJob?.label === "Reindexing"
                    ? "Reindexing..."
                    : "Reindex"}
                </Button>
                {vault.enabled && (
                  <Button
                    variant="ghost"
                    bordered
                    dark={dark}
                    disabled={!!activeJob}
                    onClick={() => {
                      setMigrateTarget((prev) => {
                        if (prev[vault.id]) {
                          const next = { ...prev };
                          delete next[vault.id];
                          return next;
                        }
                        return { ...prev, [vault.id]: { name: "", type: "", dir: "" } };
                      });
                    }}
                  >
                    {migrateTarget[vault.id] ? "Cancel Migrate" : "Migrate"}
                  </Button>
                )}
                <Button
                  variant="ghost"
                  bordered
                  dark={dark}
                  disabled={!!activeJob}
                  onClick={() => {
                    setMergeTarget((prev) =>
                      prev[vault.id] !== undefined
                        ? Object.fromEntries(Object.entries(prev).filter(([k]) => k !== vault.id))
                        : { ...prev, [vault.id]: "" },
                    );
                  }}
                >
                  {mergeTarget[vault.id] !== undefined ? "Cancel Merge" : "Merge Into..."}
                </Button>
                <Button
                  onClick={() =>
                    saveVault(vault.id, {
                      ...getEdit(vault.id),
                      type: vault.type,
                    })
                  }
                  disabled={putVault.isPending || !isDirty(vault.id) || !retentionRulesValid(getEdit(vault.id).retentionRules)}
                >
                  {putVault.isPending ? "Saving..." : "Save"}
                </Button>
              </>
            }
            headerRight={
              <span className="flex items-center gap-2">
                <NodeBadge nodeId={vault.nodeId} dark={dark} />
                {!vault.enabled && (
                  <Badge variant="ghost" dark={dark}>disabled</Badge>
                )}
                {warnings.length > 0 && (
                  <span className="text-[0.85em] text-severity-warn">
                    {warnings.join(", ")}
                  </span>
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
              <RetentionRulesEditor
                rules={edit.retentionRules}
                onChange={(rules) =>
                  setEdit(vault.id, { retentionRules: rules })
                }
                retentionPolicies={retentionPolicies}
                vaults={vaults}
                currentVaultId={vault.id}
                dark={dark}
              />
              <VaultParamsForm
                vaultType={vault.type}
                params={edit.params}
                onChange={(p) => setEdit(vault.id, { params: p })}
                dark={dark}
                vaultName={edit.name || vault.name}
              />
              {migrateTarget[vault.id] && (
                <MigrateVaultForm
                  dark={dark}
                  vault={vault}
                  target={migrateTarget[vault.id]!}
                  isPending={migrate.isPending}
                  activeJob={!!activeJob}
                  onTargetChange={(update) =>
                    setMigrateTarget((prev) => ({
                      ...prev,
                      [vault.id]: { ...prev[vault.id]!, ...update },
                    }))
                  }
                  onSubmit={async () => {
                    const mt = migrateTarget[vault.id]!;
                    const trimmedName = mt.name.trim();
                    if (!trimmedName) return;
                    const srcLabel = vault.name || vault.id;
                    if (!confirm(`Migrate "${srcLabel}" to "${trimmedName}"? This will immediately disable "${srcLabel}" and delete it after all records are moved.`)) return;
                    const destType = mt.type || undefined;
                    const params: Record<string, string> = {};
                    if (mt.dir.trim()) {
                      params["dir"] = mt.dir.trim();
                    }
                    const destParams = Object.keys(params).length > 0 ? params : undefined;
                    try {
                      const result = await migrate.mutateAsync({
                        source: vault.id,
                        destination: trimmedName,
                        destinationType: destType,
                        destinationParams: destParams,
                      });
                      setActiveJobs((prev) => ({
                        ...prev,
                        [vault.id]: {
                          jobId: result.jobId,
                          label: "Migrating",
                        },
                      }));
                      setMigrateTarget((prev) => {
                        const next = { ...prev };
                        delete next[vault.id];
                        return next;
                      });
                    } catch (err: unknown) {
                      addToast(err instanceof Error ? err.message : "Migrate failed", "error");
                    }
                  }}
                />
              )}
              {mergeTarget[vault.id] !== undefined && (
                <MergeVaultForm
                  dark={dark}
                  vault={vault}
                  selectedDestination={mergeTarget[vault.id] ?? ""}
                  vaults={vaults}
                  isPending={merge.isPending}
                  activeJob={!!activeJob}
                  onDestinationChange={(v) =>
                    setMergeTarget((prev) => ({ ...prev, [vault.id]: v }))
                  }
                  onSubmit={async () => {
                    const dest = mergeTarget[vault.id];
                    if (!dest) return;
                    const destName = vaults.find((s) => s.id === dest)?.name || dest;
                    if (!confirm(`Merge "${vault.name || vault.id}" into "${destName}"? This will immediately disable "${vault.name || vault.id}" and delete it after all records are moved.`)) return;
                    try {
                      const result = await merge.mutateAsync({
                        source: vault.id,
                        destination: dest,
                      });
                      setActiveJobs((prev) => ({
                        ...prev,
                        [vault.id]: {
                          jobId: result.jobId,
                          label: "Merging",
                        },
                      }));
                      setMergeTarget((prev) =>
                        Object.fromEntries(Object.entries(prev).filter(([k]) => k !== vault.id)),
                      );
                    } catch (err: unknown) {
                      addToast(err instanceof Error ? err.message : "Merge failed", "error");
                    }
                  }}
                />
              )}
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}
