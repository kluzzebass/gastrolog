import { useReducer, useState } from "react";
import { protoInt64 } from "@bufbuild/protobuf";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { useThemeClass } from "../../hooks/useThemeClass";
import {
  useConfig,
  usePutVault,
  usePutTier,
  useGenerateName,
} from "../../api/hooks";
import { TierConfig, TierType, RetentionRule } from "../../api/gen/gastrolog/v1/config_pb";
import { useToast } from "../Toast";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput, NumberInput, SpinnerInput } from "./FormField";
import { DropdownButton } from "./Buttons";
import { sortByName } from "../../lib/sort";
import { VaultSettingsCard } from "./VaultSettingsCard";

// ---------------------------------------------------------------------------
// Tier entry types
// ---------------------------------------------------------------------------

export type TierTypeLabel = "memory" | "file" | "cloud" | "jsonl";

export interface TierEntry {
  key: string;
  type: TierTypeLabel;
  storageClass: string;
  cloudServiceId: string;
  activeChunkClass: string;
  cacheClass: string;
  memoryBudget: string;
  rotationPolicyId: string;
  retentionPolicyId: string;
  replicationFactor: string;
  path: string;
  nodeId: string;
}

export function emptyTierEntry(type: TierTypeLabel): TierEntry {
  return {
    key: crypto.randomUUID(),
    type,
    storageClass: "",
    cloudServiceId: "",
    activeChunkClass: "",
    cacheClass: "",
    memoryBudget: "",
    rotationPolicyId: "",
    retentionPolicyId: "",
    replicationFactor: "1",
    path: "",
    nodeId: "",
  };
}

// Retention action is determined by position: last tier = expire, others = transition.
export function retentionActionForPosition(index: number, totalTiers: number): string {
  return index === totalTiers - 1 ? "expire" : "transition";
}

// ---------------------------------------------------------------------------
// Memory budget parser — "4GB" -> bigint bytes
// ---------------------------------------------------------------------------

const SIZE_MULTIPLIERS: Record<string, bigint> = {
  B: 1n,
  KB: 1024n,
  MB: 1024n * 1024n,
  GB: 1024n * 1024n * 1024n,
  TB: 1024n * 1024n * 1024n * 1024n,
};

export function parseMemoryBudget(raw: string): bigint {
  const s = raw.trim().toUpperCase();
  if (!s) return protoInt64.zero;
  const match = /^(\d+(?:\.\d+)?)\s*(TB|GB|MB|KB|B)?$/.exec(s);
  if (!match?.[1]) return protoInt64.zero;
  const num = parseFloat(match[1]);
  const unit = match[2] ?? "B";
  const mult = SIZE_MULTIPLIERS[unit] ?? 1n;
  return BigInt(Math.round(num)) * mult;
}

// ---------------------------------------------------------------------------
// Tier type enum conversion
// ---------------------------------------------------------------------------

export function tierTypeEnum(t: TierTypeLabel): TierType {
  switch (t) {
    case "memory":
      return TierType.MEMORY;
    case "file":
      return TierType.FILE;
    case "cloud":
      return TierType.CLOUD;
    case "jsonl":
      return TierType.JSONL;
  }
}

// ---------------------------------------------------------------------------
// Tier completeness check
// ---------------------------------------------------------------------------

// Extracted outside component so try/catch doesn't block the React Compiler.
async function createVaultWithTiers(
  configs: TierConfig[],
  putTier: { mutateAsync: (args: { config: TierConfig }) => Promise<unknown> },
  name: string,
  tierIds: string[],
  putVault: { mutateAsync: (args: { id: string; name: string; tierIds: string[] }) => Promise<unknown> },
): Promise<void> {
  for (const config of configs) {
    await putTier.mutateAsync({ config });
  }
  await putVault.mutateAsync({ id: "", name, tierIds });
}

function extractErrorMessage(err: unknown, fallback: string): string {
  return err instanceof Error ? err.message : fallback;
}

export function isTierComplete(tier: TierEntry, hasCloudServices: boolean): boolean {
  switch (tier.type) {
    case "memory":
      return true;
    case "file":
      return tier.storageClass !== "";
    case "cloud":
      return tier.cloudServiceId !== "" || !hasCloudServices;
    case "jsonl":
      return tier.nodeId !== "";
  }
}

// ---------------------------------------------------------------------------
// Add-form reducer
// ---------------------------------------------------------------------------

interface AddFormState {
  adding: boolean;
  name: string;
  namePlaceholder: string;
  tiers: TierEntry[];
}

const addFormInitial: AddFormState = {
  adding: false,
  name: "",
  namePlaceholder: "",
  tiers: [],
};

type AddFormAction =
  | { type: "open" }
  | { type: "close" }
  | { type: "reset" }
  | { type: "set"; patch: Partial<AddFormState> }
  | { type: "addTier"; tierType: TierTypeLabel }
  | { type: "removeTier"; key: string }
  | { type: "updateTier"; key: string; patch: Partial<TierEntry> };

function addFormReducer(state: AddFormState, action: AddFormAction): AddFormState {
  switch (action.type) {
    case "open":
      return { ...addFormInitial, adding: true };
    case "close":
    case "reset":
      return addFormInitial;
    case "set":
      return { ...state, ...action.patch };
    case "addTier":
      return { ...state, tiers: [...state.tiers, emptyTierEntry(action.tierType)] };
    case "removeTier":
      return { ...state, tiers: state.tiers.filter((t) => t.key !== action.key) };
    case "updateTier":
      return {
        ...state,
        tiers: state.tiers.map((t) =>
          t.key === action.key ? { ...t, ...action.patch } : t,
        ),
      };
  }
}

// ---------------------------------------------------------------------------
// Tier type badge label
// ---------------------------------------------------------------------------

const tierTypeLabels: Record<TierTypeLabel, string> = {
  memory: "Memory",
  file: "File",
  cloud: "Cloud",
  jsonl: "jsonl",
};

// ---------------------------------------------------------------------------
// Tier entry card
// ---------------------------------------------------------------------------

export function TierEntryCard({
  tier,
  index,
  dark,
  storageClassOptions,
  cloudServiceOptions,
  rotationPolicyOptions,
  retentionPolicyOptions,
  nodeOptions,
  vaultName,
  onUpdate,
  onRemove,
}: Readonly<{
  tier: TierEntry;
  index: number;
  dark: boolean;
  storageClassOptions: { value: string; label: string }[];
  cloudServiceOptions: { value: string; label: string }[];
  rotationPolicyOptions: { value: string; label: string }[];
  retentionPolicyOptions: { value: string; label: string }[];
  nodeOptions: { value: string; label: string }[];
  vaultName: string;
  onUpdate: (patch: Partial<TierEntry>) => void;
  onRemove: () => void;
}>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`border rounded px-3 py-2.5 flex flex-col gap-2 ${c(
        "border-ink-border/60 bg-ink-base/40",
        "border-light-border/60 bg-light-base/40",
      )}`}
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span
            className={`text-[0.7em] font-mono tabular-nums ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            {index + 1}
          </span>
          <span
            className={`px-2 py-0.5 text-[0.7em] font-mono rounded ${c(
              "bg-copper/15 text-copper",
              "bg-copper/15 text-copper",
            )}`}
          >
            {tierTypeLabels[tier.type]}
          </span>
          {index === 0 && (
            <span
              className={`text-[0.7em] ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              hottest
            </span>
          )}
        </div>
        <button
          type="button"
          onClick={onRemove}
          className={`px-1.5 py-0.5 text-[0.8em] rounded transition-colors ${c(
            "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
            "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
          )}`}
          aria-label="Remove tier"
        >
          &times;
        </button>
      </div>

      {tier.type === "memory" && (
        <FormField label="Budget" dark={dark} description="Leave empty for system default">
          <TextInput
            value={tier.memoryBudget}
            onChange={(v) => onUpdate({ memoryBudget: v })}
            placeholder="4GB"
            dark={dark}
            mono
          />
        </FormField>
      )}

      {tier.type === "file" && (
        <FormField label="Storage Class" dark={dark}>
          {storageClassOptions.length > 0 ? (
            <SelectInput
              value={tier.storageClass}
              onChange={(v) => onUpdate({ storageClass: v })}
              options={[
                { value: "", label: "Select storage class..." },
                ...storageClassOptions,
              ]}
              dark={dark}
            />
          ) : (
            <NumberInput
              value={tier.storageClass}
              onChange={(v) => onUpdate({ storageClass: v })}
              placeholder="0"
              dark={dark}
              min={0}
            />
          )}
        </FormField>
      )}

      {tier.type === "cloud" && (
        <>
          <FormField label="Cloud Storage" dark={dark}>
            {cloudServiceOptions.length > 0 ? (
              <SelectInput
                value={tier.cloudServiceId}
                onChange={(v) => onUpdate({ cloudServiceId: v })}
                options={[
                  { value: "", label: "Select cloud storage..." },
                  ...cloudServiceOptions,
                ]}
                dark={dark}
              />
            ) : (
              <TextInput
                value={tier.cloudServiceId}
                onChange={(v) => onUpdate({ cloudServiceId: v })}
                placeholder="Cloud storage ID"
                dark={dark}
                mono
              />
            )}
          </FormField>
          <div className="grid grid-cols-2 gap-2">
            <FormField label="Active Chunk Class" dark={dark}>
              {storageClassOptions.length > 0 ? (
                <SelectInput
                  value={tier.activeChunkClass}
                  onChange={(v) => onUpdate({ activeChunkClass: v })}
                  options={[{ value: "", label: "Select..." }, ...storageClassOptions]}
                  dark={dark}
                />
              ) : (
                <NumberInput
                  value={tier.activeChunkClass}
                  onChange={(v) => onUpdate({ activeChunkClass: v })}
                  placeholder="0"
                  dark={dark}
                  min={0}
                />
              )}
            </FormField>
            <FormField label="Cache Class" dark={dark}>
              {storageClassOptions.length > 0 ? (
                <SelectInput
                  value={tier.cacheClass}
                  onChange={(v) => onUpdate({ cacheClass: v })}
                  options={[{ value: "", label: "Select..." }, ...storageClassOptions]}
                  dark={dark}
                />
              ) : (
                <NumberInput
                  value={tier.cacheClass}
                  onChange={(v) => onUpdate({ cacheClass: v })}
                  placeholder="0"
                  dark={dark}
                  min={0}
                />
              )}
            </FormField>
          </div>
        </>
      )}

      {tier.type === "jsonl" && (
        <>
          <FormField label="Node" dark={dark}>
            <SelectInput
              value={tier.nodeId}
              onChange={(v) => onUpdate({ nodeId: v })}
              options={[
                { value: "", label: "Select node..." },
                ...nodeOptions,
              ]}
              dark={dark}
            />
          </FormField>
          <FormField label="Path" dark={dark} description="Relative to node home">
            <TextInput
              value={tier.path}
              onChange={(v) => onUpdate({ path: v })}
              placeholder={`jsonl/${vaultName || "vault"}/sink_${String(index + 1)}.jsonl`}
              dark={dark}
              mono
            />
          </FormField>
        </>
      )}

      {tier.type !== "jsonl" && rotationPolicyOptions.length > 0 && (
        <FormField label="Rotation Policy" dark={dark}>
          <SelectInput
            value={tier.rotationPolicyId}
            onChange={(v) => onUpdate({ rotationPolicyId: v })}
            options={[
              { value: "", label: "None" },
              ...rotationPolicyOptions,
            ]}
            dark={dark}
          />
        </FormField>
      )}

      {tier.type !== "jsonl" && retentionPolicyOptions.length > 0 && (
        <FormField label="Retention Policy" dark={dark}>
          <SelectInput
            value={tier.retentionPolicyId}
            onChange={(v) => onUpdate({ retentionPolicyId: v })}
            options={[
              { value: "", label: "None" },
              ...retentionPolicyOptions,
            ]}
            dark={dark}
          />
        </FormField>
      )}

      {tier.type !== "jsonl" && (
        <FormField label="Replication Factor" dark={dark} description="1 = no replication, 3+ = fault tolerant">
          <SpinnerInput
            value={tier.replicationFactor}
            onChange={(v) => onUpdate({ replicationFactor: v })}
            dark={dark}
            min={1}
            skip={[2]}
          />
        </FormField>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function VaultsSettings({ dark, expandTarget, onExpandTargetConsumed, onOpenInspector }: Readonly<{ dark: boolean; expandTarget?: string | null; onExpandTargetConsumed?: () => void; onOpenInspector?: (inspectorParam: string) => void }>) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putVault = usePutVault();
  const putTier = usePutTier();
  const { addToast } = useToast();

  const { isExpanded, toggle: toggleCard, setExpandedCards } = useExpandedCards();
  const generateName = useGenerateName();

  const [addForm, dispatchAdd] = useReducer(addFormReducer, addFormInitial);
  const [isCreating, setIsCreating] = useState(false);

  const configVaults = config?.vaults;
  const vaults = configVaults ?? [];
  const existingNames = new Set(vaults.map((s) => s.name));
  const effectiveName = addForm.name.trim() || addForm.namePlaceholder || "vault";
  const nameConflict = existingNames.has(effectiveName);
  const tiers = config?.tiers ?? [];
  const routes = config?.routes ?? [];

  // Derive storage class options with node availability.
  const nodeNameMap = new Map((config?.nodeConfigs ?? []).map((n) => [n.id, n.name || n.id]));
  const storageClassOptions = (() => {
    const classNodes = new Map<number, string[]>();
    for (const nsc of config?.nodeStorageConfigs ?? []) {
      const nodeName = nodeNameMap.get(nsc.nodeId) || nsc.nodeId;
      for (const area of nsc.areas) {
        const nodes = classNodes.get(area.storageClass);
        if (nodes) {
          if (!nodes.includes(nodeName)) {
            nodes.push(nodeName);
          }
        } else {
          classNodes.set(area.storageClass, [nodeName]);
        }
      }
    }
    return [...classNodes.entries()]
      .toSorted(([a], [b]) => a - b)
      .map(([sc, nodes]) => ({
        value: String(sc),
        label: `Class ${String(sc)} — ${nodes.toSorted().join(", ")}`,
      }));
  })();

  // Derive cloud storage options
  const cloudServiceOptions = (config?.cloudServices ?? [])
    .slice()
    .sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id))
    .map((cs) => ({ value: cs.id, label: cs.name || cs.id }));

  // Derive rotation policy options
  const rotationPolicyOptions = (config?.rotationPolicies ?? [])
    .slice()
    .sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id))
    .map((rp) => ({ value: rp.id, label: rp.name || rp.id }));

  // Derive retention policy options
  const retentionPolicyOptions = (config?.retentionPolicies ?? [])
    .slice()
    .sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id))
    .map((rp) => ({ value: rp.id, label: rp.name || rp.id }));

  // Validation: at least one tier, all tiers complete, no name conflict
  const allTiersComplete = addForm.tiers.length > 0 && addForm.tiers.every((t) => isTierComplete(t, cloudServiceOptions.length > 0));
  const createDisabled = nameConflict || !allTiersComplete;

  // Auto-expand a vault when navigated to from another view.
  const [consumedExpandTarget, setConsumedExpandTarget] = useState<string | null>(null);
  if (expandTarget && expandTarget !== consumedExpandTarget && configVaults && configVaults.length > 0) {
    setConsumedExpandTarget(expandTarget);
    const match = configVaults.find((s) => (s.name || s.id) === expandTarget);
    if (match) {
      setExpandedCards((prev) => ({ ...prev, [match.id]: true }));
    }
    onExpandTargetConsumed?.();
  }

  const handleCreate = () => {
    const name = addForm.name.trim() || addForm.namePlaceholder || "vault";

    // Build tier configs outside try/catch (React Compiler can't optimize
    // conditional expressions inside try/catch).
    const tierConfigs = addForm.tiers.map((tier, i) => {
      const tierId = crypto.randomUUID();
      return {
        tierId,
        config: new TierConfig({
          id: tierId,
          name: tier.type,
          type: tierTypeEnum(tier.type),
          storageClass: tier.type === "file" ? parseInt(tier.storageClass, 10) || 0 : 0,
          cloudServiceId: tier.type === "cloud" ? tier.cloudServiceId : "",
          activeChunkClass: tier.type === "cloud" ? parseInt(tier.activeChunkClass, 10) || 0 : 0,
          cacheClass: tier.type === "cloud" ? parseInt(tier.cacheClass, 10) || 0 : 0,
          memoryBudgetBytes: tier.type === "memory" ? parseMemoryBudget(tier.memoryBudget) : protoInt64.zero,
          rotationPolicyId: tier.rotationPolicyId,
          retentionRules: tier.retentionPolicyId
            ? [new RetentionRule({ retentionPolicyId: tier.retentionPolicyId, action: retentionActionForPosition(i, addForm.tiers.length) })]
            : [],
          replicationFactor: parseInt(tier.replicationFactor, 10) || 1,
        }),
      };
    });

    const tierIds = tierConfigs.map((tc) => tc.tierId);
    const configs = tierConfigs.map((tc) => tc.config);
    setIsCreating(true);
    createVaultWithTiers(configs, putTier, name, tierIds, putVault).then(
      () => { setIsCreating(false); addToast(`Vault "${name}" created`, "info"); dispatchAdd({ type: "reset" }); },
      (err: unknown) => { setIsCreating(false); addToast(extractErrorMessage(err, "Failed to create vault"), "error"); },
    );
  };

  const isPending = isCreating || putVault.isPending || putTier.isPending;

  return (
    <SettingsSection
      addLabel="Add Vault"
      adding={addForm.adding}
      onToggleAdd={() => {
        if (!addForm.adding) {
          dispatchAdd({ type: "open" });
          generateName.mutateAsync().then((n) => dispatchAdd({ type: "set", patch: { namePlaceholder: n } }));
        } else {
          dispatchAdd({ type: "close" });
        }
      }}
      isLoading={isLoading}
      isEmpty={vaults.length === 0}
      emptyMessage='No vaults configured. Click "Add Vault" to create one.'
      dark={dark}
    >
      {addForm.adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => dispatchAdd({ type: "close" })}
          onCreate={handleCreate}
          isPending={isPending}
          createDisabled={createDisabled}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={addForm.name}
              onChange={(v) => dispatchAdd({ type: "set", patch: { name: v } })}
              placeholder={addForm.namePlaceholder || "vault"}
              dark={dark}
            />
          </FormField>

          {/* Tiers section */}
          <div className="flex flex-col gap-2 pt-1">
            <div className="flex items-center justify-between">
              <span
                className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
              >
                Tiers
                {addForm.tiers.length === 0 && (
                  <span
                    className={`ml-2 text-[0.9em] font-normal ${c("text-text-ghost", "text-light-text-ghost")}`}
                  >
                    at least one required
                  </span>
                )}
              </span>
              <DropdownButton
                label="+ Add Tier"
                items={[
                  { value: "memory", label: "Memory" },
                  { value: "file", label: "File" },
                  { value: "cloud", label: "Cloud" },
                ]}
                onSelect={(v) => dispatchAdd({ type: "addTier", tierType: v as TierTypeLabel })}
                dark={dark}
              />
            </div>

            {addForm.tiers.length > 0 && (
              <p
                className={`text-[0.75em] leading-snug ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                First tier is hottest. Data migrates down the list as it ages.
              </p>
            )}

            <div className="flex flex-col gap-2">
              {addForm.tiers.map((tier, i) => (
                <TierEntryCard
                  key={tier.key}
                  tier={tier}
                  index={i}
                  dark={dark}
                  storageClassOptions={storageClassOptions}
                  cloudServiceOptions={cloudServiceOptions}
                  rotationPolicyOptions={rotationPolicyOptions}
                  retentionPolicyOptions={retentionPolicyOptions}
                  nodeOptions={(config?.nodeConfigs ?? []).map((n) => ({ value: n.id, label: n.name || n.id })).sort((a, b) => a.label.localeCompare(b.label))}
                  vaultName={addForm.name || addForm.namePlaceholder || ""}
                  onUpdate={(patch) =>
                    dispatchAdd({ type: "updateTier", key: tier.key, patch })
                  }
                  onRemove={() => dispatchAdd({ type: "removeTier", key: tier.key })}
                />
              ))}
            </div>
          </div>
        </AddFormCard>
      )}

      {sortByName(vaults).map((vault) => (
        <VaultSettingsCard
          key={vault.id}
          vault={vault}
          vaults={vaults}
          tiers={tiers}
          routes={routes}
          nodeConfigs={config?.nodeConfigs ?? []}
          nodeStorageConfigs={config?.nodeStorageConfigs ?? []}
          storageClassOptions={storageClassOptions}
          cloudServiceOptions={cloudServiceOptions}
          rotationPolicyOptions={rotationPolicyOptions}
          retentionPolicyOptions={retentionPolicyOptions}
          dark={dark}
          expanded={isExpanded(vault.id)}
          onToggle={() => toggleCard(vault.id)}
          onOpenInspector={onOpenInspector}
        />
      ))}
    </SettingsSection>
  );
}
