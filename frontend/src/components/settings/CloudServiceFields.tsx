import { FormField, TextInput, TextArea, NumberInput } from "./FormField";

interface CloudServiceFieldValues {
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

interface CloudServiceFieldsProps {
  values: CloudServiceFieldValues;
  onChange: (patch: Partial<CloudServiceFieldValues>) => void;
  dark: boolean;
}

/**
 * Provider-specific fields for cloud service configuration.
 * Used by both the add form and the edit card.
 */
export function CloudServiceFields({
  values,
  onChange,
  dark,
}: Readonly<CloudServiceFieldsProps>) {
  const isS3 = values.provider === "s3";
  const isGCS = values.provider === "gcs";
  const isAzure = values.provider === "azure";

  return (
    <>
      {/* Bucket / Container */}
      {isAzure ? (
        <FormField label="Container" dark={dark}>
          <TextInput
            value={values.container}
            onChange={(v) => onChange({ container: v })}
            dark={dark}
          />
        </FormField>
      ) : (
        <FormField label="Bucket" dark={dark}>
          <TextInput
            value={values.bucket}
            onChange={(v) => onChange({ bucket: v })}
            dark={dark}
          />
        </FormField>
      )}

      {/* Region (S3/GCS only) */}
      {(isS3 || isGCS) && (
        <FormField label="Region" dark={dark}>
          <TextInput
            value={values.region}
            onChange={(v) => onChange({ region: v })}
            dark={dark}
          />
        </FormField>
      )}

      {/* Endpoint (S3 only — for S3-compatible like MinIO) */}
      {isS3 && (
        <FormField
          label="Endpoint"
          dark={dark}
          description="For S3-compatible services (e.g. MinIO). Leave empty for AWS S3."
        >
          <TextInput
            value={values.endpoint}
            onChange={(v) => onChange({ endpoint: v })}
            dark={dark}
          />
        </FormField>
      )}

      {/* S3 credentials */}
      {isS3 && (
        <>
          <FormField label="Access Key" dark={dark}>
            <TextInput
              value={values.accessKey}
              onChange={(v) => onChange({ accessKey: v })}
              dark={dark}
              mono
            />
          </FormField>
          <FormField label="Secret Key" dark={dark}>
            <TextInput
              value={values.secretKey}
              onChange={(v) => onChange({ secretKey: v })}
              dark={dark}
              mono
            />
          </FormField>
        </>
      )}

      {/* Azure credentials */}
      {isAzure && (
        <FormField label="Connection String" dark={dark}>
          <TextInput
            value={values.connectionString}
            onChange={(v) => onChange({ connectionString: v })}
            dark={dark}
            mono
          />
        </FormField>
      )}

      {/* GCS credentials */}
      {isGCS && (
        <FormField label="Credentials JSON" dark={dark}>
          <TextArea
            value={values.credentialsJson}
            onChange={(v) => onChange({ credentialsJson: v })}
            dark={dark}
            rows={4}
          />
        </FormField>
      )}

      {/* Storage class (optional) */}
      <FormField
        label="Storage Class"
        dark={dark}
        description="Cloud provider storage class (e.g. STANDARD, NEARLINE). Leave empty for default."
      >
        <TextInput
          value={values.storageClass}
          onChange={(v) => onChange({ storageClass: v })}
          dark={dark}
        />
      </FormField>

      {/* Active Chunk Class */}
      <FormField
        label="Active Chunk Class"
        dark={dark}
        description="Local storage class for active chunks. Lower = faster storage."
      >
        <NumberInput
          value={values.activeChunkClass}
          onChange={(v) => onChange({ activeChunkClass: v })}
          dark={dark}
          min={0}
        />
      </FormField>

      {/* Cache Class */}
      <FormField
        label="Cache Class"
        dark={dark}
        description="Local storage class for cached sealed chunks. Lower = faster storage."
      >
        <NumberInput
          value={values.cacheClass}
          onChange={(v) => onChange({ cacheClass: v })}
          dark={dark}
          min={0}
        />
      </FormField>
    </>
  );
}

export type { CloudServiceFieldValues };
