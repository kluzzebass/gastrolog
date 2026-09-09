import { encode } from "../../api/glid";
import type { CloudService } from "../../api/gen/gastrolog/v1/storage_pb";
import { useState } from "react";
import {
  usePutCloudService,
  useDeleteCloudService,
} from "../../api/hooks";
import { useTestCloudService } from "../../api/hooks/useVaults";
import { useEditState } from "../../hooks/useEditState";
import { useThemeClass } from "../../hooks/useThemeClass";
import { endpointBlocked } from "../../utils/endpointScheme";
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
  cloudStorageClass: string;
}

interface CloudServiceEdit {
  name: string;
  provider: string;
  bucket: string;
  region: string;
  endpoint: string;
  accessKey: string;
  secretKey: string;
  credentialsConfigured: boolean;
  clearCredentials: boolean;
  container: string;
  connectionString: string;
  credentialsJson: string;
  archivalMode: string;
  transitions: TransitionEdit[];
  restoreSpeed: string;
  restoreDays: number;
  suspectGraceDays: number;
  reconcileSchedule: string;
}

/**
 * Builds the PutCloudService payload for an edited service. Credentials go
 * out exactly as typed: an empty one was never displayed and never changed,
 * and the server reads that as "keep the stored credential".
 */
/** Whether the operator typed credentials for a connection test to use. */
function testCredentialsTyped(e: CloudServiceEdit): boolean {
  return (
    e.accessKey !== "" ||
    e.secretKey !== "" ||
    e.connectionString !== "" ||
    e.credentialsJson !== ""
  );
}

/** Whether an unsaved edit changes where a connection test would go. */
function destinationEdited(service: CloudService, e: CloudServiceEdit): boolean {
  return (
    e.provider !== service.provider ||
    e.bucket !== service.bucket ||
    e.region !== service.region ||
    e.endpoint !== service.endpoint ||
    e.container !== service.container
  );
}

export function cloudServiceSaveRequest(id: string, e: CloudServiceEdit) {
  return {
    id,
    clearCredentials: e.clearCredentials,
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
      cloudStorageClass: t.cloudStorageClass,
    })),
    restoreSpeed: e.restoreSpeed,
    restoreDays: e.restoreDays,
    suspectGraceDays: e.suspectGraceDays,
    reconcileSchedule: e.reconcileSchedule,
  };
}

export function CloudServiceCard({
  service,
  dark,
  expanded,
  onToggle,
}: Readonly<CloudServiceCardProps>) {
  const c = useThemeClass(dark);
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
    // Credentials are never delivered to the browser; the fields start
    // empty and only a value the operator types is sent.
    accessKey: "",
    secretKey: "",
    credentialsConfigured: service.credentialsConfigured,
    clearCredentials: false,
    container: service.container,
    connectionString: "",
    credentialsJson: "",
    archivalMode: service.archivalMode || "none",
    transitions: service.transitions.map((t) => ({
      after: t.after,
      cloudStorageClass: t.cloudStorageClass,
    })),
    restoreSpeed: service.restoreSpeed || "",
    restoreDays: service.restoreDays || 7,
    suspectGraceDays: service.suspectGraceDays || 7,
    reconcileSchedule: service.reconcileSchedule || "0 3 * * *",
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(encode(service.id));
  const endpointInvalid = endpointBlocked(edit.provider, edit.endpoint);
  // A test that supplies no credentials of its own runs against the saved
  // service in full — destination included — so unsaved destination edits
  // are not what got tested. Say so rather than let a green result be read
  // as a verdict on the changes on screen.
  const testIgnoresEdits =
    !testCredentialsTyped(edit) && destinationEdited(service, edit);

  const { handleSave, handleDelete } = useCrudHandlers({
    mutation: putCloudService,
    deleteMutation: deleteCloudService,
    label: "Cloud Storage",
    onSaveTransform: cloudServiceSaveRequest,
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
                  // Naming the saved service tests it as saved, using the
                  // credentials the browser was never given. Supplying
                  // credentials below instead tests exactly what was typed,
                  // against the destination typed with them.
                  cloudServiceId: encode(service.id),
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
            disabled={testCloud.isPending || !edit.provider || endpointInvalid}
          >
            {testCloud.isPending ? "Testing..." : "Test Connection"}
          </Button>
          {testIgnoresEdits && !testResult && (
            <span className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
              Tests the saved configuration — enter credentials to test these changes.
            </span>
          )}
          {testResult && (
            <span className={`text-[0.8em] ${testResult.success ? "text-green-400" : "text-severity-error"}`}>
              {testResult.message}
            </span>
          )}
          <Button
            onClick={() => handleSave(encode(service.id), edit)}
            disabled={putCloudService.isPending || !isDirty(encode(service.id)) || endpointInvalid}
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
