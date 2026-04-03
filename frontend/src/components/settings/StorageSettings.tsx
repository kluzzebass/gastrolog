import { useReducer, useState } from "react";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import {
  useConfig,
  usePutCloudService,
  useSetNodeStorageConfig,
  useGenerateName,
} from "../../api/hooks";
import { useSettings } from "../../api/hooks/useSettings";
import { useToast } from "../Toast";
import { useThemeClass } from "../../hooks/useThemeClass";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput, SpinnerInput } from "./FormField";
import { sortByName } from "../../lib/sort";
import { CloudServiceCard } from "./CloudServiceCard";
import { FileStorageCard } from "./FileStorageCard";
import { CloudServiceFields } from "./CloudServiceFields";
import { Button } from "./Buttons";
import { NodeSelect } from "./NodeSelect";
import { useTestCloudService } from "../../api/hooks/useVaults";

// ─── Cloud Storage Add Form ──────────────────────────────────

interface AddFormState {
  adding: boolean;
  name: string;
  namePlaceholder: string;
  provider: string;
  bucket: string;
  region: string;
  endpoint: string;
  accessKey: string;
  secretKey: string;
  container: string;
  connectionString: string;
  credentialsJson: string;
  archivalMode: string;
  transitions: Array<{ after: string; storageClass: string }>;
  restoreTier: string;
  restoreDays: number;
  suspectGraceDays: number;
  reconcileSchedule: string;
}

const addFormInitial: AddFormState = {
  adding: false,
  name: "",
  namePlaceholder: "",
  provider: "s3",
  bucket: "",
  region: "",
  endpoint: "",
  accessKey: "",
  secretKey: "",
  container: "",
  connectionString: "",
  credentialsJson: "",
  archivalMode: "none",
  transitions: [],
  restoreTier: "",
  restoreDays: 7,
  suspectGraceDays: 7,
  reconcileSchedule: "0 3 * * *",
};

type AddFormAction =
  | { type: "open" }
  | { type: "close" }
  | { type: "reset" }
  | { type: "set"; patch: Partial<AddFormState> };

function addFormReducer(state: AddFormState, action: AddFormAction): AddFormState {
  switch (action.type) {
    case "open":
      return { ...addFormInitial, adding: true };
    case "close":
    case "reset":
      return addFormInitial;
    case "set":
      return { ...state, ...action.patch };
  }
}

// ─── Helpers ─────────────────────────────────────────────────

// ─── Component ───────────────────────────────────────────────

export function StorageSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const { data: settingsData } = useSettings();
  const putCloudService = usePutCloudService();
  const setNodeStorage = useSetNodeStorageConfig();
  const { addToast } = useToast();
  const { isExpanded, toggle: toggleCard } = useExpandedCards();
  const generateName = useGenerateName();

  const [addForm, dispatchAdd] = useReducer(addFormReducer, addFormInitial);

  const cloudServices = config?.cloudServices ?? [];
  const nodeStorageConfigs = config?.nodeStorageConfigs ?? [];
  const nodeConfigs = config?.nodeConfigs ?? [];
  const localNodeId = settingsData?.nodeId ?? "";

  const existingNames = new Set(cloudServices.map((s) => s.name));
  const effectiveName = addForm.name.trim() || addForm.namePlaceholder || "cloud-service";
  const nameConflict = existingNames.has(effectiveName);

  const nodeNameMap = new Map(nodeConfigs.map((n) => [n.id, n.name || n.id]));
  const resolveNodeName = (nodeId: string) => nodeNameMap.get(nodeId) || nodeId;

  const handleCreate = async () => {
    const name = addForm.name.trim() || addForm.namePlaceholder || "cloud-service";
    try {
      await putCloudService.mutateAsync({
        id: "",
        name,
        provider: addForm.provider,
        bucket: addForm.bucket,
        region: addForm.region,
        endpoint: addForm.endpoint,
        accessKey: addForm.accessKey,
        secretKey: addForm.secretKey,
        container: addForm.container,
        connectionString: addForm.connectionString,
        credentialsJson: addForm.credentialsJson,
      });
      addToast(`Cloud storage "${name}" created`, "info");
      dispatchAdd({ type: "reset" });
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create cloud storage", "error");
    }
  };

  // ─── Add/Remove file storage on any node ────────────────────

  const handleRemoveStorage = async (nodeId: string, storageId: string) => {
    const nsc = nodeStorageConfigs.find((n) => n.nodeId === nodeId);
    const currentStorages = nsc?.fileStorages ?? [];
    const updated = currentStorages.filter((a) => a.id !== storageId);
    try {
      await setNodeStorage.mutateAsync({
        nodeId,
        fileStorages: updated.map((a) => ({
          id: a.id,
          storageClass: a.storageClass,
          name: a.name,
          path: a.path,
          memoryBudgetBytes: a.memoryBudgetBytes,
        })),
      });
      addToast("File storage removed", "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to remove file storage", "error");
    }
  };

  const handleUpdateStorage = async (nodeId: string, storageId: string, edit: { name: string; path: string; storageClass: string }) => {
    const nsc = nodeStorageConfigs.find((n) => n.nodeId === nodeId);
    const currentStorages = nsc?.fileStorages ?? [];
    const updated = currentStorages.map((a) => {
      if (a.id !== storageId) return { id: a.id, storageClass: a.storageClass, name: a.name, path: a.path, memoryBudgetBytes: a.memoryBudgetBytes };
      return {
        id: a.id,
        storageClass: parseInt(edit.storageClass, 10) || 0,
        name: edit.name,
        path: edit.path,
        memoryBudgetBytes: a.memoryBudgetBytes,
      };
    });
    try {
      await setNodeStorage.mutateAsync({ nodeId, fileStorages: updated });
      addToast("File storage updated", "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to update file storage", "error");
    }
  };

  // ─── File storage add form ──────────────────────────────────

  const [addingStorage, setAddingStorage] = useState(false);
  const [storageNodeId, setStorageNodeId] = useState("");
  const [storagePath, setStoragePath] = useState("");
  const [storageClass, setStorageClass] = useState("");
  const [storageName, setStorageName] = useState("");
  const [storageNamePlaceholder, setStorageNamePlaceholder] = useState("");
  const openStorageForm = () => {
    setAddingStorage(true);
    setStorageNodeId(localNodeId);
    generateName.mutateAsync().then(setStorageNamePlaceholder);
  };
  const resetStorageForm = () => {
    setAddingStorage(false);
    setStorageNodeId("");
    setStoragePath("");
    setStorageClass("");
    setStorageName("");
    setStorageNamePlaceholder("");
  };

  const handleCreateStorage = async () => {
    const targetNodeId = storageNodeId || localNodeId;
    const effectiveName = storageName.trim() || storageNamePlaceholder || "file-storage";
    const path = storagePath.trim() || `storage/${effectiveName}`;
    const cls = parseInt(storageClass, 10);
    if (!targetNodeId || isNaN(cls)) return;

    const name = storageName.trim() || storageNamePlaceholder || "file-storage";

    const newStorage = {
      id: crypto.randomUUID(),
      storageClass: cls,
      name,
      path,
      memoryBudgetBytes: BigInt(0),
    };

    const nsc = nodeStorageConfigs.find((n) => n.nodeId === targetNodeId);
    const existingStorages = nsc?.fileStorages ?? [];
    const updated = [...existingStorages.map((a) => ({
      id: a.id,
      storageClass: a.storageClass,
      name: a.name,
      path: a.path,
      memoryBudgetBytes: a.memoryBudgetBytes,
    })), newStorage];

    try {
      await setNodeStorage.mutateAsync({
        nodeId: targetNodeId,
        fileStorages: updated,
      });
      addToast(`File storage "${name}" created`, "info");
      resetStorageForm();
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create file storage", "error");
    }
  };

  return (
    <div className="flex flex-col gap-8">
      {/* ── Section 1: File Storage ──────────────────────────── */}
      <div>
        <h3 className={`font-display text-[1.1em] font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}>
          File Storage
        </h3>
        <p className={`text-[0.85em] mb-4 ${c("text-text-muted", "text-light-text-muted")}`}>
          Local disk storage declared per node. Storage class ranks speed: lower = faster.
        </p>

        {/* Add File Storage button */}
        {!isLoading && !addingStorage && (
          <div className="flex items-center justify-end mb-5">
            <Button onClick={openStorageForm}>Add File Storage</Button>
          </div>
        )}

        {/* Add File Storage form */}
        {addingStorage && (
          <div className="mb-4">
            <AddFormCard
              dark={dark}
              onCancel={resetStorageForm}
              onCreate={handleCreateStorage}
              isPending={setNodeStorage.isPending}
              createDisabled={!storageClass.trim() || isNaN(parseInt(storageClass, 10))}
            >
              <NodeSelect value={storageNodeId} onChange={setStorageNodeId} dark={dark} />
              <FormField label="Name" dark={dark}>
                <TextInput
                  value={storageName}
                  onChange={setStorageName}
                  placeholder={storageNamePlaceholder || "file-storage"}
                  dark={dark}
                />
              </FormField>
              <FormField label="Path" dark={dark} description="Relative to the node's home directory, or absolute if starting with /.">
                <TextInput
                  value={storagePath}
                  onChange={setStoragePath}
                  placeholder={`storage/${storageName.trim() || storageNamePlaceholder || ""}`}
                  dark={dark}
                  mono
                />
              </FormField>
              <FormField label="Storage Class" dark={dark} description="Numeric rank. Lower = faster (e.g. 1 for NVMe, 3 for HDD).">
                <SpinnerInput
                  value={storageClass}
                  onChange={setStorageClass}
                  dark={dark}
                  min={0}
                />
              </FormField>
            </AddFormCard>
          </div>
        )}

        {isLoading && (
          <div className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            Loading...
          </div>
        )}
        {(() => {
          // Flatten all file storages across all nodes into a single sorted list.
          const allStorages = nodeStorageConfigs.flatMap((nsc) =>
            nsc.fileStorages.map(( fs) => ({
              fs,
              nodeId: nsc.nodeId,
              nodeName: resolveNodeName(nsc.nodeId),
            })),
          ).sort((a, b) => (a.fs.name || a.fs.id).localeCompare(b.fs.name || b.fs.id));

          if (!isLoading && allStorages.length === 0 && !addingStorage) {
            return (
              <div className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
                No file storage configured. Click &quot;Add File Storage&quot; to create one.
              </div>
            );
          }

          return (
            <div className="flex flex-col gap-2">
              {allStorages.map(({ fs, nodeId, nodeName }) => (
                <FileStorageCard
                  key={fs.id}
                  fs={fs}
                  nodeName={nodeName}
                  dark={dark}
                  expanded={isExpanded(`storage-${fs.id}`)}
                  onToggle={() => toggleCard(`storage-${fs.id}`)}
                  onSave={async (storageId, edit) => {
                    await handleUpdateStorage(nodeId, storageId, edit);
                  }}
                  onDelete={async (storageId) => {
                    await handleRemoveStorage(nodeId, storageId);
                  }}
                  saving={setNodeStorage.isPending}
                />
              ))}
            </div>
          );
        })()}
      </div>

      {/* ── Section 2: Cloud Storage ─────────────────────────── */}
      <div>
        <h3 className={`font-display text-[1.1em] font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}>
          Cloud Storage
        </h3>
        <p className={`text-[0.85em] mb-4 ${c("text-text-muted", "text-light-text-muted")}`}>
          Cluster-wide cloud storage endpoints. Tiers reference a cloud service by ID.
        </p>
        <SettingsSection
          addLabel="Add Cloud Storage"
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
          isEmpty={cloudServices.length === 0}
          emptyMessage='No cloud storage configured. Click "Add Cloud Storage" to create one.'
          dark={dark}
        >
          {addForm.adding && (
            <AddFormCard
              dark={dark}
              onCancel={() => dispatchAdd({ type: "close" })}
              onCreate={handleCreate}
              isPending={putCloudService.isPending}
              createDisabled={nameConflict || (addForm.provider === "azure" ? !addForm.container.trim() : !addForm.bucket.trim())}
            >
              <FormField label="Name" dark={dark}>
                <TextInput
                  value={addForm.name}
                  onChange={(v) => dispatchAdd({ type: "set", patch: { name: v } })}
                  placeholder={addForm.namePlaceholder || "cloud-service"}
                  dark={dark}
                />
              </FormField>
              <FormField label="Provider" dark={dark}>
                <SelectInput
                  value={addForm.provider}
                  onChange={(v) => dispatchAdd({ type: "set", patch: { provider: v } })}
                  options={[
                    { value: "s3", label: "S3" },
                    { value: "gcs", label: "GCS" },
                    { value: "azure", label: "Azure" },
                    { value: "memory", label: "Memory" },
                  ]}
                  dark={dark}
                />
              </FormField>
              <CloudServiceFields
                values={addForm}
                onChange={(patch) => dispatchAdd({ type: "set", patch })}
                dark={dark}
              />
              <TestCloudButton provider={addForm.provider} values={addForm} dark={dark} />
            </AddFormCard>
          )}

          {sortByName(cloudServices).map((svc) => (
            <CloudServiceCard
              key={svc.id}
              service={svc}
              dark={dark}
              expanded={isExpanded(svc.id)}
              onToggle={() => toggleCard(svc.id)}
            />
          ))}
        </SettingsSection>
      </div>
    </div>
  );
}

function TestCloudButton({
  provider,
  values,
  dark,
}: Readonly<{
  provider: string;
  values: { bucket: string; region: string; endpoint: string; accessKey: string; secretKey: string; container: string; connectionString: string; credentialsJson: string };
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const testCloud = useTestCloudService();
  const [result, setResult] = useState<{ success: boolean; message: string } | null>(null);

  const hasRequired =
    provider !== "" &&
    ((provider === "s3" && values.bucket !== "") ||
      (provider === "azure" && values.container !== "" && values.connectionString !== "") ||
      (provider === "gcs" && values.bucket !== "") ||
      provider === "memory");

  return (
    <div className="flex items-center gap-3">
      <button
        type="button"
        disabled={testCloud.isPending || !hasRequired}
        onClick={() => {
          setResult(null);
          testCloud.mutate(
            {
              type: "file",
              params: {
                sealed_backing: provider,
                bucket: values.bucket,
                region: values.region,
                endpoint: values.endpoint,
                access_key: values.accessKey,
                secret_key: values.secretKey,
                container: values.container,
                connection_string: values.connectionString,
                credentials_json: values.credentialsJson,
              },
            },
            {
              onSuccess: (resp) => setResult({ success: resp.success, message: resp.message }),
              onError: (err) => setResult({ success: false, message: err instanceof Error ? err.message : String(err) }),
            },
          );
        }}
        className={`px-3 py-1.5 text-[0.8em] font-medium rounded border transition-colors ${c(
          "bg-ink-surface border-ink-border text-text-bright hover:border-copper-dim disabled:opacity-50",
          "bg-light-surface border-light-border text-light-text-bright hover:border-copper disabled:opacity-50",
        )}`}
      >
        {testCloud.isPending ? "Testing..." : "Test Connection"}
      </button>
      {result && (
        <span className={`text-[0.8em] ${result.success ? "text-green-400" : "text-severity-error"}`}>
          {result.message}
        </span>
      )}
    </div>
  );
}
