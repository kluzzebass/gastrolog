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
];

function providerLabel(provider: string): string {
  switch (provider) {
    case "s3": return "S3";
    case "gcs": return "GCS";
    case "azure": return "Azure";
    default: return provider || "unknown";
  }
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
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(service.id);

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
    }),
    onDeleteTransform: (id) => ({ id }),
    clearEdit,
  });

  return (
    <SettingsCard
      id={service.name || service.id}
      typeBadge={providerLabel(service.provider)}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={() => handleDelete(service.id)}
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
            onClick={() => handleSave(service.id, edit)}
            disabled={putCloudService.isPending || !isDirty(service.id)}
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
            onChange={(v) => setEdit(service.id, { name: v })}
            dark={dark}
          />
        </FormField>
        <FormField label="Provider" dark={dark}>
          <SelectInput
            value={edit.provider}
            onChange={(v) => setEdit(service.id, { provider: v })}
            options={providerOptions}
            dark={dark}
          />
        </FormField>

          <CloudServiceFields
          values={edit}
          onChange={(patch) => setEdit(service.id, patch)}
          dark={dark}
        />
      </div>
    </SettingsCard>
  );
}
