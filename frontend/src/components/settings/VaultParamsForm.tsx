import { useState } from "react";
import { FormField, TextInput, TextArea, SelectInput } from "./FormField";
import { useTestCloudService } from "../../api/hooks/useVaults";
import { useThemeClass } from "../../hooks/useThemeClass";

interface VaultParamsFormProps {
  vaultType: string;
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
  vaultName?: string;
}

const sealedBackingOptions = [
  { value: "", label: "Local (default)" },
  { value: "s3", label: "S3 / S3-compatible" },
  { value: "azure", label: "Azure Blob Storage" },
  { value: "gcs", label: "Google Cloud Storage" },
];

export function VaultParamsForm({
  vaultType,
  params,
  onChange,
  dark,
  vaultName,
}: Readonly<VaultParamsFormProps>) {
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });
  const get = (key: string) => params[key] ?? "";

  if (vaultType === "file") {
    const backing = get("sealed_backing");
    return (
      <div className="flex flex-col gap-3">
        <FormField
          label="Directory"
          description="Path where chunk files are stored"
          dark={dark}
        >
          <TextInput
            value={get("dir")}
            onChange={(v) => set("dir", v)}
            placeholder={vaultName ? `vaults/${vaultName}` : ""}
            dark={dark}
            mono
          />
        </FormField>
        <FormField
          label="Sealed Backing"
          description="Where sealed chunks are stored after sealing"
          dark={dark}
        >
          <SelectInput
            value={backing}
            onChange={(v) => set("sealed_backing", v)}
            options={sealedBackingOptions}
            dark={dark}
          />
        </FormField>
        {(backing === "s3" || backing === "gcs") && (
          <FormField label="Bucket" dark={dark}>
            <TextInput
              value={get("bucket")}
              onChange={(v) => set("bucket", v)}
              placeholder=""
              dark={dark}
              mono
            />
          </FormField>
        )}
        {backing === "azure" && (
          <FormField label="Container" dark={dark}>
            <TextInput
              value={get("container")}
              onChange={(v) => set("container", v)}
              placeholder=""
              dark={dark}
              mono
            />
          </FormField>
        )}
        {backing === "s3" && (
          <>
            <FormField label="Region" dark={dark}>
              <TextInput
                value={get("region")}
                onChange={(v) => set("region", v)}
                placeholder=""
                dark={dark}
              />
            </FormField>
            <FormField label="Access Key" dark={dark}>
              <TextInput
                value={get("access_key")}
                onChange={(v) => set("access_key", v)}
                placeholder=""
                dark={dark}
                mono
              />
            </FormField>
            <FormField label="Secret Key" dark={dark}>
              <TextInput
                value={get("secret_key")}
                onChange={(v) => set("secret_key", v)}
                placeholder=""
                dark={dark}
                mono
              />
            </FormField>
          </>
        )}
        {backing === "azure" && (
          <FormField label="Connection String" dark={dark}>
            <TextArea
              value={get("connection_string")}
              onChange={(v) => set("connection_string", v)}
              placeholder=""
              dark={dark}
              rows={2}
            />
          </FormField>
        )}
        {backing === "gcs" && (
          <FormField
            label="Credentials JSON"
            description="Service account key (JSON). Falls back to Application Default Credentials if empty."
            dark={dark}
          >
            <TextArea
              value={get("credentials_json")}
              onChange={(v) => set("credentials_json", v)}
              placeholder=""
              dark={dark}
              rows={3}
            />
          </FormField>
        )}
        {(backing === "s3" || backing === "gcs") && (
          <FormField
            label="Endpoint"
            description={backing === "s3"
              ? "Custom endpoint for S3-compatible services (MinIO, R2, B2, etc.)"
              : "Custom endpoint for GCS-compatible services"}
            dark={dark}
          >
            <TextInput
              value={get("endpoint")}
              onChange={(v) => set("endpoint", v)}
              placeholder=""
              dark={dark}
              mono
            />
          </FormField>
        )}
        {backing && backing !== "" && (
          <TestCloudServiceButton type="file" params={params} dark={dark} />
        )}
      </div>
    );
  }

  if (vaultType === "memory") {
    return null;
  }

  return null;
}

function TestCloudServiceButton({
  type,
  params,
  dark,
}: Readonly<{
  type: string;
  params: Record<string, string>;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const testCloud = useTestCloudService();
  const [testResult, setTestResult] = useState<{
    success: boolean;
    message: string;
  } | null>(null);

  const provider = params.sealed_backing ?? params.provider ?? "";
  const hasRequired =
    provider !== "" &&
    ((provider === "s3" && (params.bucket ?? "") !== "") ||
      (provider === "azure" &&
        (params.container ?? "") !== "" &&
        (params.connection_string ?? "") !== "") ||
      (provider === "gcs" && (params.bucket ?? "") !== ""));

  return (
    <div className="flex items-center gap-3">
      <button
        type="button"
        disabled={testCloud.isPending || !hasRequired}
        onClick={() => {
          setTestResult(null);
          testCloud.mutate(
            { type, params },
            {
              onSuccess: (resp) => {
                setTestResult({
                  success: resp.success,
                  message: resp.message,
                });
              },
              onError: (err) => {
                setTestResult({
                  success: false,
                  message: err instanceof Error ? err.message : String(err),
                });
              },
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
      {testResult && (
        <span
          className={`text-[0.8em] ${
            testResult.success
              ? "text-severity-info"
              : "text-severity-error"
          }`}
        >
          {testResult.message}
        </span>
      )}
    </div>
  );
}
