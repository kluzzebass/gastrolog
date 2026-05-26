import { encode, decode } from "../../api/glid";
import { useReducer, useState } from "react";
import { protoInt64 } from "@bufbuild/protobuf";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { buildNodeNameMap, resolveNodeName } from "../../utils/nodeNames";
import {
  useConfig,
  usePutVault,
  useGenerateName,
} from "../../api/hooks";
import { VaultType, RetentionRule, VaultConfig } from "../../api/gen/gastrolog/v1/system_pb";
import { useToast } from "../Toast";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput, SpinnerInput } from "./FormField";
import { Checkbox } from "./Checkbox";
import { sortByName } from "../../lib/sort";
import { VaultSettingsCard } from "./VaultSettingsCard";
import { normalizeWriteModel, type WriteModel, writeModelSelectOptions } from "../../utils/writeModel";

// ---------------------------------------------------------------------------
// Vault storage form types
// ---------------------------------------------------------------------------

// A cloud-backed vault is a file vault with cloudServiceId set; cloud-ness
// is derived via isCloudBacked() rather than a separate type discriminator.
// See gastrolog-4k5mg.
export type VaultTypeLabel = "memory" | "file" | "jsonl";

/** Returns true if this vault is cloud-backed (file vault with a cloud service binding). */
export function isCloudBacked(v: { type: VaultTypeLabel; cloudServiceId: string }): boolean {
  return v.type === "file" && v.cloudServiceId !== "";
}

export interface StorageEntry {
  key: string;
  type: VaultTypeLabel;
  storageClass: string;
  cloudServiceId: string;
  cacheEviction: string;
  cacheBudget: string;
  cacheTTL: string;
  memoryBudget: string;
  rotationPolicyId: string;
  retentionPolicyId: string;
  retentionDisposition: string; // "delete" (default) | "route"
  replicationFactor: string;
  writeModel: WriteModel;
  path: string;
  nodeId: string;
}

export function emptyStorageEntry(type: VaultTypeLabel): StorageEntry {
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
    retentionPolicyId: "",
    retentionDisposition: "delete",
    replicationFactor: "1",
    writeModel: "chunk_append",
    path: "",
    nodeId: "",
  };
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
// VaultType label ↔ enum conversion
// ---------------------------------------------------------------------------

export function vaultTypeEnum(t: VaultTypeLabel): VaultType {
  switch (t) {
    case "memory":
      return VaultType.MEMORY;
    case "file":
      return VaultType.FILE;
    case "jsonl":
      return VaultType.JSONL;
  }
}

function cloudBindingDescription(locked: boolean, hasCloudServices: boolean): string {
  if (locked) {
    return "Cloud binding is fixed at vault creation. To change it, create a new vault and migrate data via retention routing.";
  }
  if (!hasCloudServices) {
    return "No cloud services configured — leave empty for local-only";
  }
  return "Optional — select to make this vault cloud-backed";
}

function extractErrorMessage(err: unknown, fallback: string): string {
  return err instanceof Error ? err.message : fallback;
}

export function isStorageComplete(s: StorageEntry, _hasCloudServices: boolean): boolean {
  switch (s.type) {
    case "memory":
      return true;
    case "file":
      // Single storage class for both local-only and cloud-backed file
      // vaults — the active chunk and warm cache live at the same chunkDir
      // path, so no separate "active" or "cache" class is meaningful.
      return s.storageClass !== "";
    case "jsonl":
      return s.nodeId !== "";
  }
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
  nodeOptions,
  vaultName,
  maxRF,
  cloudLocked,
  onTypeChange,
  onUpdate,
}: Readonly<{
  storage: StorageEntry;
  dark: boolean;
  storageClassOptions: { value: string; label: string }[];
  cloudServiceOptions: { value: string; label: string }[];
  rotationPolicyOptions: { value: string; label: string }[];
  retentionPolicyOptions: { value: string; label: string }[];
  nodeOptions: { value: string; label: string }[];
  vaultName: string;
  maxRF?: number;
  // cloudLocked freezes the Cloud Storage selector. The backend rejects
  // cloud_service_id changes on existing vaults (gastrolog-3ul0s) — to
  // change cloud binding, create a new vault and route data via
  // retention. The Add form leaves this false; edit-existing passes true.
  cloudLocked?: boolean;
  onTypeChange?: (t: VaultTypeLabel) => void;
  onUpdate: (patch: Partial<StorageEntry>) => void;
}>) {
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
              { value: "jsonl", label: "JSONL sink" },
            ]}
            dark={dark}
          />
        </FormField>
      )}

      {storage.type === "memory" && (
        <FormField label="Budget" dark={dark} description="Leave empty for system default">
          <TextInput
            value={storage.memoryBudget}
            onChange={(v) => onUpdate({ memoryBudget: v })}
            placeholder="4GB"
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
            description={cloudBindingDescription(!!cloudLocked, cloudServiceOptions.length > 0)}
          >
            <SelectInput
              value={storage.cloudServiceId}
              onChange={(v) => onUpdate({ cloudServiceId: v })}
              options={[
                { value: "", label: "Local-only" },
                ...cloudServiceOptions,
              ]}
              dark={dark}
              disabled={cloudLocked}
            />
          </FormField>

          <FormField label="Storage Class" dark={dark}>
            {storageClassOptions.length > 0 ? (
              <SelectInput
                value={storage.storageClass}
                onChange={(v) => onUpdate({ storageClass: v })}
                options={[
                  { value: "", label: "Select storage class..." },
                  ...storageClassOptions,
                ]}
                dark={dark}
              />
            ) : (
              <SpinnerInput
                value={storage.storageClass}
                onChange={(v) => onUpdate({ storageClass: v })}
                dark={dark}
                min={0}
              />
            )}
          </FormField>

          {/* Cache eviction tuning is only meaningful on cloud-backed
              vaults — local-only vaults have nothing to evict (sealed
              chunks ARE the data, not a cache). */}
          {storage.cloudServiceId !== "" && (
            <>
              <FormField label="Cache Eviction" dark={dark}>
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
              <FormField label="Cache Budget" dark={dark}>
                <TextInput
                  value={storage.cacheBudget}
                  onChange={(v) => onUpdate({ cacheBudget: v })}
                  placeholder="1GiB"
                  dark={dark}
                  mono
                  examples={["500MB", "1GiB", "5GB", "10GB"]}
                />
              </FormField>
              {(storage.cacheEviction === "ttl") && (
                <FormField label="Cache TTL" dark={dark}>
                  <TextInput
                    value={storage.cacheTTL}
                    onChange={(v) => onUpdate({ cacheTTL: v })}
                    placeholder=""
                    dark={dark}
                    mono
                    examples={["1h", "12h", "1d", "7d"]}
                  />
                </FormField>
              )}
            </>
          )}
        </>
      )}

      {storage.type === "jsonl" && (
        <>
          <FormField label="Node" dark={dark}>
            <SelectInput
              value={storage.nodeId}
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
              value={storage.path}
              onChange={(v) => onUpdate({ path: v })}
              placeholder={`jsonl/${vaultName || "vault"}.jsonl`}
              dark={dark}
              mono
            />
          </FormField>
        </>
      )}

      {storage.type !== "jsonl" && rotationPolicyOptions.length > 0 && (
        <FormField label="Rotation Policy" dark={dark}>
          <SelectInput
            value={storage.rotationPolicyId}
            onChange={(v) => onUpdate({ rotationPolicyId: v })}
            options={[
              { value: "", label: "None" },
              ...rotationPolicyOptions,
            ]}
            dark={dark}
          />
        </FormField>
      )}

      {storage.type !== "jsonl" && retentionPolicyOptions.length > 0 && (
        <FormField label="Retention Policy" dark={dark}>
          <SelectInput
            value={storage.retentionPolicyId}
            onChange={(v) => onUpdate({ retentionPolicyId: v })}
            options={[
              { value: "", label: "None" },
              ...retentionPolicyOptions,
            ]}
            dark={dark}
          />
        </FormField>
      )}

      {storage.type !== "jsonl" && (
        <FormField
          label="Retention Disposition"
          dark={dark}
          description="What happens to records when retention triggers. 'Delete' frees storage immediately. 'Route' sends records through the routing engine — only enable if you have an archival route configured for this vault, otherwise records may cascade unexpectedly."
        >
          <SelectInput
            value={storage.retentionDisposition || "delete"}
            onChange={(v) => onUpdate({ retentionDisposition: v })}
            options={[
              { value: "delete", label: "Delete records on retention" },
              { value: "route", label: "Send records to routing engine" },
            ]}
            dark={dark}
          />
        </FormField>
      )}

      {storage.type !== "jsonl" && (
        <FormField
          label="Write Model"
          dark={dark}
          description="Chunk append uses the leader-driven active-chunk path. Sequenced enables destination-vault sequencing and spool-first ingest."
        >
          <SelectInput
            value={storage.writeModel}
            onChange={(v) => onUpdate({ writeModel: normalizeWriteModel(v) })}
            options={writeModelSelectOptions()}
            dark={dark}
          />
        </FormField>
      )}

      {storage.type !== "jsonl" && (
        <FormField label="Replication Factor" dark={dark} description="1 = none, 2 = redundant, 3+ = fault tolerant">
          <SpinnerInput
            value={storage.replicationFactor}
            onChange={(v) => onUpdate({ replicationFactor: v })}
            dark={dark}
            min={1}
            max={maxRF}
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

  // Derive storage class options with node availability.
  const nodeNameMap = buildNodeNameMap(config?.nodeConfigs ?? []);
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
        label: `Class ${String(sc)} — ${nodes.toSorted().join(", ")}`,
      }));
  })();

  // Compute eligible file storage count per storage class (for RF max).
  // Same-node replication is valid — count file storages, not nodes.
  const classStorageCount = new Map<number, number>();
  for (const nsc of config?.nodeStorageConfigs ?? []) {
    for (const fs of nsc.fileStorages) {
      classStorageCount.set(fs.storageClass, (classStorageCount.get(fs.storageClass) ?? 0) + 1);
    }
  }
  const totalNodes = config?.nodeConfigs.length ?? 1;
  const maxRFForStorage = (s: { type: string; storageClass: string }) => {
    if (s.type === "memory") return totalNodes;
    if (s.type === "jsonl") return 1;
    // Single storage class for all file vaults (local-only and cloud-backed).
    const sc = parseInt(s.storageClass, 10) || 0;
    if (sc === 0) return 1; // no class selected yet
    return classStorageCount.get(sc) ?? 1;
  };

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

  // Auto-expand a vault when navigated to from another view.
  const [consumedExpandTarget, setConsumedExpandTarget] = useState<string | null>(null);
  if (expandTarget && expandTarget !== consumedExpandTarget && configVaults && configVaults.length > 0) {
    setConsumedExpandTarget(expandTarget);
    const match = configVaults.find((s) => (s.name || encode(s.id)) === expandTarget);
    if (match) {
      setExpandedCards((prev) => ({ ...prev, [encode(match.id)]: true }));
    }
    onExpandTargetConsumed?.();
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
      cacheBudget: cloudBacked ? (storage.cacheBudget || "") : "",
      cacheTtl: cloudBacked ? (storage.cacheTTL || "") : "",
      memoryBudgetBytes: storage.type === "memory" ? parseMemoryBudget(storage.memoryBudget) : protoInt64.zero,
      rotationPolicyId: storage.rotationPolicyId ? decode(storage.rotationPolicyId) : new Uint8Array(0),
      retentionRules: storage.retentionPolicyId
        ? [new RetentionRule({ retentionPolicyId: decode(storage.retentionPolicyId) })]
        : [],
      retentionDisposition: storage.type !== "jsonl" ? (storage.retentionDisposition || "delete") : "",
      replicationFactor: parseInt(storage.replicationFactor, 10) || 1,
      writeModel: storage.type !== "jsonl" ? storage.writeModel : "",
      path: storage.type === "jsonl" ? storage.path : "",
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
            nodeOptions={(config?.nodeConfigs ?? []).map((n) => ({ value: encode(n.id), label: n.name || encode(n.id) })).sort((a, b) => a.label.localeCompare(b.label))}
            vaultName={addForm.name || addForm.namePlaceholder || ""}
            maxRF={maxRFForStorage(addForm.storage)}
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
