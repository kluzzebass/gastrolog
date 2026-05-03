import { encode } from "../../api/glid";
import type { CloudService } from "../../api/gen/gastrolog/v1/storage_pb";
import { useState } from "react";
import {
  usePutCloudService,
  useDeleteCloudService,
} from "../../api/hooks";
import { useTestCloudService } from "../../api/hooks/useVaults";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { CloudServiceFields } from "./CloudServiceFields";
import { Button } from "./Buttons";

interface CloudServiceCardProps {
  service: CloudService;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
}

const providerOptions = [
  { value: "s3", label: "S3" },
  { value: "gcs", label: "GCS" },
  { value: "azure", label: "Azure" },
  { value: "memory", label: "Memory" },
];

function providerLabel(provider: string): string {
  switch (provider) {
    case "s3": return "S3";
    case "gcs": return "GCS";
    case "azure": return "Azure";
    case "memory": return "Memory";
    default: return provider || "unknown";
  }
}

interface TransitionEdit {
  after: string;
  storageClass: string;
}

interface CloudServiceEdit {
  name: string;
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
  transitions: TransitionEdit[];
  restoreTier: string;
  restoreDays: number;
  suspectGraceDays: number;
  reconcileSchedule: string;
}

export function CloudServiceCard({
  service,
  dark,
  expanded,
  onToggle,
}: Readonly<CloudServiceCardProps>) {
  const putCloudService = usePutCloudService();
  const deleteCloudService = useDeleteCloudService();
  const testCloud = useTestCloudService();
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);

  const defaults = (_id: string): CloudServiceEdit => ({
    name: service.name,
    provider: service.provider,
    bucket: service.bucket,
    region: service.region,
    endpoint: service.endpoint,
    accessKey: service.accessKey,
    secretKey: service.secretKey,
    container: service.container,
    connectionString: service.connectionString,
    credentialsJson: service.credentialsJson,
    archivalMode: service.archivalMode || "none",
    transitions: service.transitions.map((t) => ({
      after: t.after,
      storageClass: t.storageClass,
    })),
    restoreTier: service.restoreTier || "",
    restoreDays: service.restoreDays || 7,
    suspectGraceDays: service.suspectGraceDays || 7,
    reconcileSchedule: service.reconcileSchedule || "0 3 * * *",
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(encode(service.id));

  const { handleSave, handleDelete } = useCrudHandlers({
    mutation: putCloudService,
    deleteMutation: deleteCloudService,
    label: "Cloud Storage",
    onSaveTransform: (id, e: CloudServiceEdit) => ({
      id,
      name: e.name,
      provider: e.provider,
      bucket: e.bucket,
      region: e.region,
      endpoint: e.endpoint,
      accessKey: e.accessKey,
      secretKey: e.secretKey,
      container: e.container,
      connectionString: e.connectionString,
      credentialsJson: e.credentialsJson,
      archivalMode: e.archivalMode,
      transitions: e.transitions.map((t) => ({
        after: t.after,
        storageClass: t.storageClass,
      })),
      restoreTier: e.restoreTier,
      restoreDays: e.restoreDays,
      suspectGraceDays: e.suspectGraceDays,
      reconcileSchedule: e.reconcileSchedule,
    }),
    onDeleteTransform: (id) => ({ id }),
  });

  return (
    <SettingsCard
      id={service.name || encode(service.id)}
      typeBadge={providerLabel(service.provider)}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={() => handleDelete(encode(service.id))}
      deleteLabel="Delete"
      footer={
        <div className="flex items-center gap-3">
          <Button
            onClick={() => {
              setTestResult(null);
              testCloud.mutate(
                {
                  type: "file",
                  params: {
                    sealed_backing: edit.provider,
                    bucket: edit.bucket,
                    region: edit.region,
                    endpoint: edit.endpoint,
                    access_key: edit.accessKey,
                    secret_key: edit.secretKey,
                    container: edit.container,
                    connection_string: edit.connectionString,
                    credentials_json: edit.credentialsJson,
                  },
                },
                {
                  onSuccess: (resp) => setTestResult({ success: resp.success, message: resp.message }),
                  onError: (err) => setTestResult({ success: false, message: err instanceof Error ? err.message : String(err) }),
                },
              );
            }}
            disabled={testCloud.isPending || !edit.provider}
          >
            {testCloud.isPending ? "Testing..." : "Test Connection"}
          </Button>
          {testResult && (
            <span className={`text-[0.8em] ${testResult.success ? "text-green-400" : "text-severity-error"}`}>
              {testResult.message}
            </span>
          )}
          <Button
            onClick={() => handleSave(encode(service.id), edit)}
            disabled={putCloudService.isPending || !isDirty(encode(service.id))}
          >
            {putCloudService.isPending ? "Saving..." : "Save"}
          </Button>
        </div>
      }
    >
      <div className="flex flex-col gap-3">
        <FormField label="Name" dark={dark}>
          <TextInput
            value={edit.name}
            onChange={(v) => setEdit(encode(service.id), { name: v })}
            dark={dark}
          />
        </FormField>
        <FormField label="Provider" dark={dark}>
          <SelectInput
            value={edit.provider}
            onChange={(v) => setEdit(encode(service.id), { provider: v })}
            options={providerOptions}
            dark={dark}
          />
        </FormField>

          <CloudServiceFields
          values={edit}
          onChange={(patch) => setEdit(encode(service.id), patch)}
          dark={dark}
        />
      </div>
    </SettingsCard>
  );
}
