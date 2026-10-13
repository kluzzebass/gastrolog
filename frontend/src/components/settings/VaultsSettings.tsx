import { encode, decode } from "../../api/glid";
import { useReducer, useState } from "react";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { buildNodeNameMap, resolveNodeName } from "../../utils/nodeNames";
import {
  useConfig,
  usePutVault,
  useGenerateName,
} from "../../api/hooks";
import { RetentionRule, VaultConfig } from "../../api/gen/gastrolog/v1/system_pb";
import { useToast } from "../Toast";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput, SpinnerInput } from "./FormField";
import { Checkbox } from "./Checkbox";
import { HelpLink } from "../HelpLink";
import { useThemeClass } from "../../hooks/useThemeClass";
import { EligibilitySelect, type EligibilityOption } from "./EligibilitySelect";
import { TRANSFER_HELP_REF, transferSourceIneligibility, transferTargetOptions } from "./transferEligibility";
import { sortByName } from "../../lib/sort";
import { VaultSettingsCard } from "./VaultSettingsCard";
import { classStorageCounts, plural, rfCap, type RFCap } from "./vaultPlacement";
import {
  isCloudBacked,
  isStorageComplete,
  storageIncompleteReason,
  vaultTypeEnum,
  type StorageEntry,
  type VaultTypeLabel,
} from "./vaultStorageEntry";

// ---------------------------------------------------------------------------
// Vault storage form types
// ---------------------------------------------------------------------------

function emptyStorageEntry(type: VaultTypeLabel): StorageEntry {
  return {
    key: crypto.randomUUID(),
    type,
    storageClass: "",
    cloudServiceId: "",
    cacheEviction: "lru",
    cacheBudget: "",
    cacheTTL: "",
    memoryBudget: "",
    rotationPolicyId: "",
    retentionPolicyIds: [],
    retentionDisposition: "delete",
    retentionTransferTarget: "",
    replicationFactor: "1",
  };
}

/** Mirrors the backend's DefaultVaultMemoryBudget: what an empty Budget resolves to. */
export const DEFAULT_VAULT_MEMORY_BUDGET = "1GiB";

const CLOUD_STORAGE_HELP_REF = "storage-config#cloud-storage";
const FILE_STORAGE_HELP_REF = "storage-config#file-storage";
const POLICIES_HELP_REF = "vaults-config#rotation-and-retention-policies";
const CACHE_HELP_REF = "storage-cloud#settings";

function extractErrorMessage(err: unknown, fallback: string): string {
  return err instanceof Error ? err.message : fallback;
}

// ---------------------------------------------------------------------------
// Add-form reducer
// ---------------------------------------------------------------------------

interface AddFormState {
  adding: boolean;
  name: string;
  namePlaceholder: string;
  enabled: boolean;
  storage: StorageEntry;
}

const addFormInitial: AddFormState = {
  adding: false,
  name: "",
  namePlaceholder: "",
  enabled: true,
  storage: emptyStorageEntry("file"),
};

type AddFormAction =
  | { type: "open" }
  | { type: "close" }
  | { type: "reset" }
  | { type: "set"; patch: Partial<Omit<AddFormState, "storage">> }
  | { type: "setType"; vaultType: VaultTypeLabel }
  | { type: "updateStorage"; patch: Partial<StorageEntry> };

function addFormReducer(state: AddFormState, action: AddFormAction): AddFormState {
  switch (action.type) {
    case "open":
      return { ...addFormInitial, adding: true };
    case "close":
    case "reset":
      return addFormInitial;
    case "set":
      return { ...state, ...action.patch };
    case "setType":
      return { ...state, storage: emptyStorageEntry(action.vaultType) };
    case "updateStorage":
      return { ...state, storage: { ...state.storage, ...action.patch } };
  }
}


// ---------------------------------------------------------------------------
// Vault storage form
// ---------------------------------------------------------------------------

export function VaultStorageForm({
  storage,
  dark,
  storageClassOptions,
  cloudServiceOptions,
  rotationPolicyOptions,
  retentionPolicyOptions,
  transferTargetOptions,
  nodeOptions,
  vaultName,
  cap,
  cloudLocked,
  onTypeChange,
  onUpdate,
}: Readonly<{
  storage: StorageEntry;
  dark: boolean;
  storageClassOptions: EligibilityOption[];
  cloudServiceOptions: EligibilityOption[];
  rotationPolicyOptions: EligibilityOption[];
  retentionPolicyOptions: EligibilityOption[];
  transferTargetOptions: EligibilityOption[];
  nodeOptions: { value: string; label: string }[];
  vaultName: string;
  cap: RFCap;
  // cloudLocked freezes the Cloud Storage selector. The backend rejects
  // cloud_service_id changes on existing vaults — to change cloud binding,
  // create a new vault and route data via retention. The Add form leaves
  // this false; edit-existing passes true.
  cloudLocked?: boolean;
  onTypeChange?: (t: VaultTypeLabel) => void;
  onUpdate: (patch: Partial<StorageEntry>) => void;
}>) {
  const cacheReason = storage.cloudServiceId === "" ? "Only a cloud-backed vault has a warm cache." : undefined;
  let cacheTtlReason = cacheReason;
  if (cacheTtlReason === undefined && storage.cacheEviction !== "ttl") {
    cacheTtlReason = "Applies to TTL eviction only.";
  }
  return (
    <div className="flex flex-col gap-2">
      {onTypeChange && (
        <FormField label="Storage Type" dark={dark}>
          <SelectInput
            value={storage.type}
            onChange={(v) => onTypeChange(v as VaultTypeLabel)}
            options={[
              { value: "memory", label: "Memory" },
              { value: "file", label: "File" },
            ]}
            dark={dark}
          />
        </FormField>
      )}

      {storage.type === "memory" && (
        <FormField label="Budget" dark={dark} description="In-memory cap for the vault's records; empty uses the default">
          <TextInput
            value={storage.memoryBudget}
            onChange={(v) => onUpdate({ memoryBudget: v })}
            placeholder={DEFAULT_VAULT_MEMORY_BUDGET}
            dark={dark}
            mono
          />
        </FormField>
      )}

      {storage.type === "file" && (
        <>
          {/* Cloud Storage selector — when set, the file vault becomes
              cloud-backed (sealed chunks upload to S3/etc; the active
              chunk and a warm cache stay on local disk). The storage
              class governs the local placement either way. */}
          <FormField
            label="Cloud Storage"
            dark={dark}
            description={cloudLocked ? undefined : "Optional — select to make this vault cloud-backed"}
            disabledReason={cloudLocked ?"Fixed at vault creation — to change it, create a new vault and migrate data via retention routing." : undefined}
            helpRef={CLOUD_STORAGE_HELP_REF}
          >
            <EligibilitySelect
              value={storage.cloudServiceId}
              onChange={(v) => onUpdate({ cloudServiceId: v })}
              options={cloudServiceOptions}
              placeholder="Local-only"
              emptyMessage={cloudLocked ? undefined : "No cloud storage configured — add one under Storage; until then the vault is local-only."}
              helpRef={CLOUD_STORAGE_HELP_REF}
              dark={dark}
            />
          </FormField>

          <FormField label="Storage Class" helpRef={FILE_STORAGE_HELP_REF} dark={dark}>
            <EligibilitySelect
              value={storage.storageClass}
              onChange={(v) => onUpdate({ storageClass: v })}
              options={storageClassOptions}
              placeholder="Select storage class..."
              emptyMessage="No file storage configured — add one under Storage; a file vault needs a storage class to be placed."
              helpRef={FILE_STORAGE_HELP_REF}
              dark={dark}
            />
          </FormField>

          <FormField label="Cache Eviction" helpRef={CACHE_HELP_REF} disabledReason={cacheReason} dark={dark}>
            <SelectInput
              value={storage.cacheEviction || "lru"}
              onChange={(v) => onUpdate({ cacheEviction: v })}
              options={[
                { value: "lru", label: "LRU — evict oldest when over budget" },
                { value: "ttl", label: "TTL — evict after max age" },
              ]}
              dark={dark}
            />
          </FormField>
          <FormField label="Cache Budget" helpRef={CACHE_HELP_REF} disabledReason={cacheReason} dark={dark}>
            <TextInput
              value={storage.cacheBudget}
              onChange={(v) => onUpdate({ cacheBudget: v })}
              placeholder="1GiB"
              dark={dark}
              mono
              examples={["500MB", "1GiB", "5GB", "10GB"]}
            />
          </FormField>
          <FormField label="Cache TTL" helpRef={CACHE_HELP_REF} disabledReason={cacheTtlReason} dark={dark}>
            <TextInput
              value={storage.cacheTTL}
              onChange={(v) => onUpdate({ cacheTTL: v })}
              placeholder=""
              dark={dark}
              mono
              examples={["1h", "12h", "1d", "7d"]}
            />
          </FormField>
        </>
      )}

      <FormField label="Rotation Policy" helpRef={POLICIES_HELP_REF} dark={dark}>
        <EligibilitySelect
          value={storage.rotationPolicyId}
          onChange={(v) => onUpdate({ rotationPolicyId: v })}
          options={rotationPolicyOptions}
          placeholder="None"
          emptyMessage="No rotation policies configured — create one under Rotation Policies; without one the active chunk is not rotated automatically."
          helpRef={POLICIES_HELP_REF}
          dark={dark}
        />
      </FormField>

      <RetentionPoliciesEditor
        attached={storage.retentionPolicyIds}
        onChange={(ids) => onUpdate({ retentionPolicyIds: ids })}
        options={retentionPolicyOptions}
        dark={dark}
      />

      <FormField
        label="Retention Disposition"
        dark={dark}
        description="What retention does with aged-out chunks: delete, route the records, or transfer the chunk unchanged."
      >
        <EligibilitySelect
          value={storage.retentionDisposition || "delete"}
          onChange={(v) => onUpdate({ retentionDisposition: v })}
          options={[
            { value: "delete", label: "Delete records on retention" },
            { value: "route", label: "Send records to routing engine" },
            {
              value: "transfer",
              label: "Transfer records to another vault unchanged",
              ineligibleReason: transferSourceIneligibility(storage),
            },
          ]}
          helpRef={TRANSFER_HELP_REF}
          dark={dark}
        />
      </FormField>

      {storage.retentionDisposition === "transfer" && (
        <FormField label="Transfer Target" dark={dark}>
          <EligibilitySelect
            value={storage.retentionTransferTarget}
            onChange={(v) => onUpdate({ retentionTransferTarget: v })}
            options={transferTargetOptions}
            placeholder="Select target vault..."
            emptyMessage="No eligible targets — transfer requires a different file or memory vault that is not cloud-backed."
            helpRef={TRANSFER_HELP_REF}
            dark={dark}
          />
        </FormField>
      )}

      <FormField
        label="Replication Factor"
        dark={dark}
        description={`1 = none, 2 = redundant, 3+ = fault tolerant. Cap ${String(cap.max)}: ${cap.cause}.`}
        helpRef="storage-config#how-vaults-use-file-storages"
      >
        <SpinnerInput
          value={storage.replicationFactor}
          onChange={(v) => onUpdate({ replicationFactor: v })}
          dark={dark}
          min={1}
          max={cap.max}
        />
      </FormField>
    </div>
  );
}

// RetentionPoliciesEditor renders a flat checkbox list — every attached
// policy is explicit. The wire shape is a list of RetentionRule in
// attachment order; this component keeps that order, appending newly
// checked policies at the end.
function RetentionPoliciesEditor({
  attached,
  onChange,
  options,
  dark,
}: Readonly<{
  attached: string[];
  onChange: (ids: string[]) => void;
  options: { value: string; label: string }[];
  dark: boolean;
}>) {
  const selected = new Set(attached);
  const toggle = (id: string) => {
    onChange(selected.has(id) ? attached.filter((a) => a !== id) : [...attached, id]);
  };
  const n = attached.length;
  const noun = n === 1 ? "policy" : "policies";
  const description = n === 0
    ? "None attached: sealed chunks stay until the default size floor drains them."
    : `${String(n)} ${noun} attached. Every attached policy is evaluated on each sweep; the tightest bound wins.`;
  const c = useThemeClass(dark);
  if (options.length === 0) {
    return (
      <FormField label="Retention Policies" helpRef={POLICIES_HELP_REF} dark={dark}>
        <div role="note" className={`text-[0.85em] leading-relaxed ${c("text-text-normal", "text-light-text-normal")}`}>
          No retention policies configured — create one under Retention Policies; without one only the default size floor applies.{" "}
          <HelpLink helpRef={POLICIES_HELP_REF} className="text-copper">Eligibility rules</HelpLink>
        </div>
      </FormField>
    );
  }
  return (
    <FormField
      label="Retention Policies"
      helpRef={POLICIES_HELP_REF}
      dark={dark}
      description={description}
    >
      <div className="flex flex-col gap-1.5">
        {options.map((o) => (
          <Checkbox
            key={o.value}
            checked={selected.has(o.value)}
            onChange={() => toggle(o.value)}
            label={o.label}
            dark={dark}
          />
        ))}
      </div>
    </FormField>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function VaultsSettings({ dark, expandTarget, onOpenInspector }: Readonly<{ dark: boolean; expandTarget?: string | null; onOpenInspector?: (inspectorParam: string) => void }>) {
  const { data: config, isLoading } = useConfig();
  const putVault = usePutVault();
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
  const routes = config?.routes ?? [];

  // Storage class options carry the fact that caps RF: the storage count
  // in the class (same-node storages each hold a replica), plus the nodes.
  const nodeNameMap = buildNodeNameMap(config?.nodeConfigs ?? []);
  const classStorageCount = classStorageCounts(config?.nodeStorageConfigs ?? []);
  const storageClassOptions = (() => {
    const classNodes = new Map<number, string[]>();
    for (const nsc of config?.nodeStorageConfigs ?? []) {
      const nodeName = resolveNodeName(nodeNameMap, encode(nsc.nodeId));
      for (const fs of nsc.fileStorages) {
        const nodes = classNodes.get(fs.storageClass);
        if (nodes) {
          if (!nodes.includes(nodeName)) {
            nodes.push(nodeName);
          }
        } else {
          classNodes.set(fs.storageClass, [nodeName]);
        }
      }
    }
    return [...classNodes.entries()]
      .toSorted(([a], [b]) => a - b)
      .map(([sc, nodes]) => ({
        value: String(sc),
        label: `Class ${String(sc)} — ${plural(classStorageCount.get(sc) ?? 0, "file storage")} on ${nodes.toSorted().join(", ")}`,
      }));
  })();
  const totalNodes = config?.nodeConfigs.length ?? 1;

  // Derive cloud storage options
  const cloudServiceOptions = (config?.cloudServices ?? [])
    .slice()
    .sort((a, b) => (a.name || encode(a.id)).localeCompare(b.name || encode(b.id)))
    .map((cs) => ({ value: encode(cs.id), label: cs.name || encode(cs.id) }));

  // Derive rotation policy options
  const rotationPolicyOptions = (config?.rotationPolicies ?? [])
    .slice()
    .sort((a, b) => (a.name || encode(a.id)).localeCompare(b.name || encode(b.id)))
    .map((rp) => ({ value: encode(rp.id), label: rp.name || encode(rp.id) }));

  // Derive retention policy options
  const retentionPolicyOptions = (config?.retentionPolicies ?? [])
    .slice()
    .sort((a, b) => (a.name || encode(a.id)).localeCompare(b.name || encode(b.id)))
    .map((rp) => ({ value: encode(rp.id), label: rp.name || encode(rp.id) }));

  // Validation: storage shape complete, no name conflict.
  const storageComplete = isStorageComplete(addForm.storage, cloudServiceOptions.length > 0);
  const createDisabled = nameConflict || !storageComplete;
  let createDisabledReason: string | undefined;
  if (nameConflict) createDisabledReason = `A vault named "${effectiveName}" already exists`;
  else if (!storageComplete) createDisabledReason = storageIncompleteReason(addForm.storage);

  // Auto-expand a vault when navigated to from another view.
  const [consumedExpandTarget, setConsumedExpandTarget] = useState<string | null>(null);
  if (expandTarget && expandTarget !== consumedExpandTarget && configVaults && configVaults.length > 0) {
    setConsumedExpandTarget(expandTarget);
    const match = configVaults.find((s) => (s.name || encode(s.id)) === expandTarget);
    if (match) {
      setExpandedCards((prev) => ({ ...prev, [encode(match.id)]: true }));
    }
  }

  const handleCreate = () => {
    const name = addForm.name.trim() || addForm.namePlaceholder || "vault";
    const vaultIdBytes = crypto.getRandomValues(new Uint8Array(16));
    const storage = addForm.storage;
    const cloudBacked = isCloudBacked(storage);

    const vaultCfg = new VaultConfig({
      id: vaultIdBytes,
      name,
      enabled: addForm.enabled,
      type: vaultTypeEnum(storage.type),
      storageClass: storage.type === "file" ? parseInt(storage.storageClass, 10) || 0 : 0,
      cloudServiceId: cloudBacked ? decode(storage.cloudServiceId) : new Uint8Array(0),
      cacheEviction: cloudBacked ? (storage.cacheEviction || "lru") : "",
      // Expressions pass through verbatim; the server defaults an unset value
      // for the applicable vault type and rejects an explicit "0".
      cacheBudget: cloudBacked ? storage.cacheBudget : "",
      cacheTtl: cloudBacked ? storage.cacheTTL : "",
      memoryBudget: storage.type === "memory" ? storage.memoryBudget : "",
      rotationPolicyId: storage.rotationPolicyId ? decode(storage.rotationPolicyId) : new Uint8Array(0),
      retentionRules: storage.retentionPolicyIds.map((id) => new RetentionRule({ retentionPolicyId: decode(id) })),
      retentionDisposition: storage.retentionDisposition || "delete",
      retentionTransferTargetVaultId:
        storage.retentionDisposition === "transfer" && storage.retentionTransferTarget
          ? decode(storage.retentionTransferTarget)
          : new Uint8Array(0),
      replicationFactor: parseInt(storage.replicationFactor, 10) || 1,
    });

    setIsCreating(true);
    putVault.mutateAsync({ config: vaultCfg }).then(
      () => { setIsCreating(false); addToast(`Vault "${name}" created`, "info"); dispatchAdd({ type: "reset" }); },
      (err: unknown) => { setIsCreating(false); addToast(extractErrorMessage(err, "Failed to create vault"), "error"); },
    );
  };

  const isPending = isCreating || putVault.isPending;

  return (
    <SettingsSection
      addLabel="Add Vault"
      adding={addForm.adding}
      onToggleAdd={() => {
        if (!addForm.adding) {
          dispatchAdd({ type: "open" });
          generateName.mutateAsync().then((n) => dispatchAdd({ type: "set", patch: { namePlaceholder: n } })).catch(() => {});
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
          createDisabledReason={createDisabledReason}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={addForm.name}
              onChange={(v) => dispatchAdd({ type: "set", patch: { name: v } })}
              placeholder={addForm.namePlaceholder || "vault"}
              dark={dark}
            />
          </FormField>
          <Checkbox
            checked={addForm.enabled}
            onChange={(v) => dispatchAdd({ type: "set", patch: { enabled: v } })}
            label="Enabled"
            dark={dark}
          />

          <VaultStorageForm
            storage={addForm.storage}
            dark={dark}
            storageClassOptions={storageClassOptions}
            cloudServiceOptions={cloudServiceOptions}
            rotationPolicyOptions={rotationPolicyOptions}
            retentionPolicyOptions={retentionPolicyOptions}
            transferTargetOptions={transferTargetOptions(vaults)}
            nodeOptions={(config?.nodeConfigs ?? []).map((n) => ({ value: encode(n.id), label: n.name || encode(n.id) })).sort((a, b) => a.label.localeCompare(b.label))}
            vaultName={addForm.name || addForm.namePlaceholder || ""}
            cap={rfCap(addForm.storage, classStorageCount, totalNodes)}
            onTypeChange={(t) => dispatchAdd({ type: "setType", vaultType: t })}
            onUpdate={(patch) => dispatchAdd({ type: "updateStorage", patch })}
          />
        </AddFormCard>
      )}

      {sortByName(vaults).map((vault) => (
        <VaultSettingsCard
          key={encode(vault.id)}
          vault={vault}
          vaults={vaults}
          routes={routes}
          nodeConfigs={config?.nodeConfigs ?? []}
          nodeStorageConfigs={config?.nodeStorageConfigs ?? []}
          storageClassOptions={storageClassOptions}
          cloudServiceOptions={cloudServiceOptions}
          rotationPolicyOptions={rotationPolicyOptions}
          retentionPolicyOptions={retentionPolicyOptions}
          dark={dark}
          expanded={isExpanded(encode(vault.id))}
          onToggle={() => toggleCard(encode(vault.id))}
          onOpenInspector={onOpenInspector}
        />
      ))}
    </SettingsSection>
  );
}
