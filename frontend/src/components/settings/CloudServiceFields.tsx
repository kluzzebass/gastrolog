import { useThemeClass } from "../../hooks/useThemeClass";
import { FormField, TextInput, TextArea, SelectInput, NumberInput } from "./FormField";
import { Button } from "./Buttons";

interface CloudStorageTransitionEdit {
  afterDays: number;
  storageClass: string; // empty = delete
}

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
  archivalMode: string;
  transitions: CloudStorageTransitionEdit[];
  restoreTier: string;
  restoreDays: number;
  suspectGraceDays: number;
  reconcileSchedule: string;
}

interface CloudServiceFieldsProps {
  values: CloudServiceFieldValues;
  onChange: (patch: Partial<CloudServiceFieldValues>) => void;
  dark: boolean;
}

/**
 * Provider-specific fields for cloud storage configuration.
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

      {/* Archival Lifecycle */}
      <ArchivalSection values={values} onChange={onChange} dark={dark} />
    </>
  );
}

// --- Archival Lifecycle Section ---

const s3ClassOptions = [
  { value: "STANDARD_IA", label: "Standard-IA" },
  { value: "GLACIER_IR", label: "Glacier IR" },
  { value: "GLACIER", label: "Glacier" },
  { value: "DEEP_ARCHIVE", label: "Deep Archive" },
  { value: "", label: "Delete" },
];

const azureClassOptions = [
  { value: "Cool", label: "Cool" },
  { value: "Cold", label: "Cold" },
  { value: "Archive", label: "Archive" },
  { value: "", label: "Delete" },
];

const gcsClassOptions = [
  { value: "NEARLINE", label: "Nearline" },
  { value: "COLDLINE", label: "Coldline" },
  { value: "ARCHIVE", label: "Archive" },
  { value: "", label: "Delete" },
];

const memoryClassOptions = [
  { value: "cool", label: "Cool — readable, higher access cost" },
  { value: "cold", label: "Cold — offline, requires restore" },
  { value: "deep-freeze", label: "Deep Freeze — offline, slow restore" },
  { value: "", label: "Delete" },
];

const s3RestoreTierOptions = [
  { value: "Expedited", label: "Expedited (1-5 min)" },
  { value: "Standard", label: "Standard (3-5 hr)" },
  { value: "Bulk", label: "Bulk (5-12 hr)" },
];

const azureRehydrateOptions = [
  { value: "High", label: "High (< 10 hr)" },
  { value: "Standard", label: "Standard (< 15 hr)" },
];

function classOptionsForProvider(provider: string) {
  switch (provider) {
    case "s3": return s3ClassOptions;
    case "azure": return azureClassOptions;
    case "gcs": return gcsClassOptions;
    case "memory": return memoryClassOptions;
    default: return s3ClassOptions;
  }
}

function ArchivalSection({
  values,
  onChange,
  dark,
}: Readonly<{
  values: CloudServiceFieldValues;
  onChange: (patch: Partial<CloudServiceFieldValues>) => void;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const isS3 = values.provider === "s3";
  const isAzure = values.provider === "azure";
  const isGCS = values.provider === "gcs";
  const isMemory = values.provider === "memory";
  const isActive = values.archivalMode === "active";
  const classOptions = classOptionsForProvider(values.provider);

  const setTransition = (idx: number, patch: Partial<CloudStorageTransitionEdit>) => {
    const updated = values.transitions.map((t, i) =>
      i === idx ? { afterDays: patch.afterDays ?? t.afterDays, storageClass: patch.storageClass ?? t.storageClass } : t,
    );
    onChange({ transitions: updated });
  };

  const addTransition = () => {
    const existing = values.transitions;
    const last = existing.at(-1);
    const lastDays = last?.afterDays ?? 0;
    const firstClass = classOptions[0]?.value ?? "";
    onChange({
      transitions: [...existing, { afterDays: lastDays + 90, storageClass: firstClass }],
    });
  };

  const removeTransition = (idx: number) => {
    onChange({ transitions: values.transitions.filter((_, i) => i !== idx) });
  };

  return (
    <div className="flex flex-col gap-2 pt-3">
      <div className="flex items-center justify-between">
        <span className={`text-[0.85em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}>
          Archival Lifecycle
        </span>
        <SelectInput
          value={values.archivalMode || "none"}
          onChange={(v) => onChange({ archivalMode: v })}
          options={[
            { value: "none", label: "None" },
            { value: "active", label: "Active" },
          ]}
          dark={dark}
        />
      </div>

      {isActive && (
        <div className="flex flex-col gap-2">
          <p className={`text-[0.75em] leading-snug ${c("text-text-ghost", "text-light-text-ghost")}`}>
            Transitions are applied in order by chunk age.
          </p>

          {values.transitions.map((t, i) => (
            <div key={i} className="flex items-end gap-2">
              <div className="w-24">
                <FormField label={i === 0 ? "After Days" : ""} dark={dark}>
                  <NumberInput
                    value={String(t.afterDays)}
                    onChange={(v) => setTransition(i, { afterDays: parseInt(v, 10) || 0 })}
                    dark={dark}
                    min={1}
                  />
                </FormField>
              </div>
              <div className="flex-1">
                <FormField label={i === 0 ? "Storage Class" : ""} dark={dark}>
                  <SelectInput
                    value={t.storageClass}
                    onChange={(v) => setTransition(i, { storageClass: v })}
                    options={classOptions}
                    dark={dark}
                  />
                </FormField>
              </div>
              <button
                onClick={() => removeTransition(i)}
                className={`px-2 py-1 text-[0.75em] rounded transition-colors ${c(
                  "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
                  "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
                )}`}
              >
                Remove
              </button>
            </div>
          ))}

          <Button variant="ghost" dark={dark} onClick={addTransition}>
            + Add Transition
          </Button>

          {/* Restore defaults — S3 and Azure only (GCS has no restore step) */}
          {!isGCS && !isMemory && (
            <div className="flex items-end gap-2 pt-1">
              <div className="flex-1">
                <FormField label="Restore Speed" dark={dark}>
                  <SelectInput
                    value={values.restoreTier || "Standard"}
                    onChange={(v) => onChange({ restoreTier: v })}
                    options={isAzure ? azureRehydrateOptions : s3RestoreTierOptions}
                    dark={dark}
                  />
                </FormField>
              </div>
              {isS3 && (
                <div className="w-28">
                  <FormField label="Restore Days" dark={dark}>
                    <NumberInput
                      value={String(values.restoreDays || 7)}
                      onChange={(v) => onChange({ restoreDays: parseInt(v, 10) || 7 })}
                      dark={dark}
                      min={1}
                    />
                  </FormField>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export type { CloudServiceFieldValues };
