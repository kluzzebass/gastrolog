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
import { FormField, TextInput, SelectInput, NumberInput } from "./FormField";
import { sortByName } from "../../lib/sort";
import { CloudServiceCard } from "./CloudServiceCard";
import { CloudServiceFields } from "./CloudServiceFields";
import { Badge } from "../Badge";
import { Button } from "./Buttons";

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
  storageClass: string;
  activeChunkClass: string;
  cacheClass: string;
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
  storageClass: "",
  activeChunkClass: "",
  cacheClass: "",
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

  // Sort local storage configs: local node first, then alphabetical by node name.
  const sortedNodeConfigs = [...nodeStorageConfigs].sort((a, b) => {
    if (a.nodeId === localNodeId) return -1;
    if (b.nodeId === localNodeId) return 1;
    return resolveNodeName(a.nodeId).localeCompare(resolveNodeName(b.nodeId));
  });

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
        storageClass: addForm.storageClass,
        activeChunkClass: addForm.activeChunkClass ? parseInt(addForm.activeChunkClass, 10) : 0,
        cacheClass: addForm.cacheClass ? parseInt(addForm.cacheClass, 10) : 0,
      });
      addToast(`Cloud storage "${name}" created`, "info");
      dispatchAdd({ type: "reset" });
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create cloud storage", "error");
    }
  };

  // ─── Add/Remove storage area for local node ────────────────

  const localNodeConfig = nodeStorageConfigs.find((n) => n.nodeId === localNodeId);
  const localAreas = localNodeConfig?.areas ?? [];

  const handleRemoveArea = async (areaId: string) => {
    const updated = localAreas.filter((a) => a.id !== areaId);
    try {
      await setNodeStorage.mutateAsync({
        nodeId: localNodeId,
        areas: updated.map((a) => ({
          id: a.id,
          storageClass: a.storageClass,
          name: a.name,
          path: a.path,
          memoryBudgetBytes: a.memoryBudgetBytes,
        })),
      });
      addToast("Storage area removed", "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to remove storage area", "error");
    }
  };

  // ─── Storage area add form ──────────────────────────────────

  const [addingArea, setAddingArea] = useState(false);
  const [areaPath, setAreaPath] = useState("");
  const [areaClass, setAreaClass] = useState("");
  const [areaName, setAreaName] = useState("");
  const [areaNamePlaceholder, setAreaNamePlaceholder] = useState("");
  const openAreaForm = () => {
    setAddingArea(true);
    generateName.mutateAsync().then(setAreaNamePlaceholder);
  };
  const resetAreaForm = () => {
    setAddingArea(false);
    setAreaPath("");
    setAreaClass("");
    setAreaName("");
    setAreaNamePlaceholder("");
  };

  const handleCreateArea = async () => {
    const path = areaPath.trim();
    const cls = parseInt(areaClass, 10);
    if (!path || isNaN(cls)) return;

    const name = areaName.trim() || areaNamePlaceholder || "storage-area";

    const newArea = {
      id: crypto.randomUUID(),
      storageClass: cls,
      name,
      path,
      memoryBudgetBytes: BigInt(0),
    };

    const updated = [...localAreas.map((a) => ({
      id: a.id,
      storageClass: a.storageClass,
      name: a.name,
      path: a.path,
      memoryBudgetBytes: a.memoryBudgetBytes,
    })), newArea];

    try {
      await setNodeStorage.mutateAsync({
        nodeId: localNodeId,
        areas: updated,
      });
      addToast(`Storage area "${name}" created`, "info");
      resetAreaForm();
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create storage area", "error");
    }
  };

  return (
    <div className="flex flex-col gap-8">
      {/* ── Section 1: Cloud Storage ─────────────────────────── */}
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
                  ]}
                  dark={dark}
                />
              </FormField>
              <CloudServiceFields
                values={addForm}
                onChange={(patch) => dispatchAdd({ type: "set", patch })}
                dark={dark}
              />
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

      {/* ── Section 2: Local Storage ─────────────────────────── */}
      <div>
        <h3 className={`font-display text-[1.1em] font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}>
          Local Storage
        </h3>
        <p className={`text-[0.85em] mb-4 ${c("text-text-muted", "text-light-text-muted")}`}>
          Locally-attached storage resources declared per node. Storage class ranks speed: lower = faster.
        </p>

        {/* Add Storage Area button */}
        {!isLoading && localNodeId && (
          <div className="flex items-center justify-end mb-5">
            {addingArea ? (
              <Button onClick={resetAreaForm}>Cancel</Button>
            ) : (
              <Button onClick={openAreaForm}>Add Storage Area</Button>
            )}
          </div>
        )}

        {/* Add Storage Area form */}
        {addingArea && (
          <div className="mb-4">
            <AddFormCard
              dark={dark}
              onCancel={resetAreaForm}
              onCreate={handleCreateArea}
              isPending={setNodeStorage.isPending}
              createDisabled={!areaPath.trim() || !areaClass.trim() || isNaN(parseInt(areaClass, 10))}
            >
              <FormField label="Name" dark={dark}>
                <TextInput
                  value={areaName}
                  onChange={setAreaName}
                  placeholder={areaNamePlaceholder || "storage-area"}
                  dark={dark}
                />
              </FormField>
              <FormField label="Path" dark={dark} description="Relative to the node's home directory, or absolute if starting with /.">
                <TextInput
                  value={areaPath}
                  onChange={setAreaPath}
                  placeholder=""
                  dark={dark}
                  mono
                />
              </FormField>
              <FormField label="Storage Class" dark={dark} description="Numeric rank. Lower = faster (e.g. 1 for NVMe, 3 for HDD).">
                <NumberInput
                  value={areaClass}
                  onChange={setAreaClass}
                  placeholder=""
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
        {!isLoading && sortedNodeConfigs.length === 0 && !addingArea && (
          <div className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            No local storage configured. Click &quot;Add Storage Area&quot; to create one.
          </div>
        )}
        {!isLoading && sortedNodeConfigs.length > 0 && (
          <div className="flex flex-col gap-4">
            {sortedNodeConfigs.map((nsc) => {
              const isLocal = nsc.nodeId === localNodeId;
              const nodeName = resolveNodeName(nsc.nodeId);
              const areas = [...nsc.areas].sort((a, b) => a.storageClass - b.storageClass || a.name.localeCompare(b.name));
              return (
                <div
                  key={nsc.nodeId}
                  className={`border rounded-lg p-4 ${c(
                    "border-ink-border bg-ink-well",
                    "border-light-border bg-light-well",
                  )}`}
                >
                  <div className="flex items-center gap-2 mb-3">
                    <span className={`text-[0.9em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}>
                      {nodeName}
                    </span>
                    {isLocal && <Badge variant="copper" dark={dark}>this node</Badge>}
                  </div>

                  {areas.length === 0 ? (
                    <div className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
                      No storage areas configured.
                    </div>
                  ) : (
                    <div className="flex flex-col gap-2">
                      {areas.map((area) => (
                        <div
                          key={area.id}
                          className={`flex items-center gap-3 px-3 py-2 rounded border ${c(
                            "border-ink-border bg-ink-surface",
                            "border-light-border bg-light-surface",
                          )}`}
                        >
                          <Badge variant="muted" dark={dark}>
                            class {area.storageClass}
                          </Badge>
                          <span className={`text-[0.85em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}>
                            {area.name || area.id}
                          </span>
                          <span className={`text-[0.8em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                            {area.path}
                          </span>
                          <span className="flex-1" />
                          {isLocal && (
                            <button
                              onClick={() => handleRemoveArea(area.id)}
                              disabled={setNodeStorage.isPending}
                              className={`px-2 py-1 text-[0.75em] rounded transition-colors ${c(
                                "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
                                "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
                              )} disabled:opacity-50`}
                            >
                              Remove
                            </button>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
