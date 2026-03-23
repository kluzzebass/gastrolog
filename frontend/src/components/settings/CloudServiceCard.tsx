import type { CloudService } from "../../api/gen/gastrolog/v1/storage_pb";
import {
  usePutCloudService,
  useDeleteCloudService,
} from "../../api/hooks";
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
  storageClass: string;
  activeChunkClass: string;
  cacheClass: string;
}

export function CloudServiceCard({
  service,
  dark,
  expanded,
  onToggle,
}: Readonly<CloudServiceCardProps>) {
  const putCloudService = usePutCloudService();
  const deleteCloudService = useDeleteCloudService();

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
    storageClass: service.storageClass,
    activeChunkClass: service.activeChunkClass ? String(service.activeChunkClass) : "",
    cacheClass: service.cacheClass ? String(service.cacheClass) : "",
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(service.id);

  const { handleSave, handleDelete } = useCrudHandlers({
    mutation: putCloudService,
    deleteMutation: deleteCloudService,
    label: "Cloud Service",
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
      storageClass: e.storageClass,
      activeChunkClass: e.activeChunkClass ? parseInt(e.activeChunkClass, 10) : 0,
      cacheClass: e.cacheClass ? parseInt(e.cacheClass, 10) : 0,
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
        <Button
          onClick={() => handleSave(service.id, edit)}
          disabled={putCloudService.isPending || !isDirty(service.id)}
        >
          {putCloudService.isPending ? "Saving..." : "Save"}
        </Button>
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
