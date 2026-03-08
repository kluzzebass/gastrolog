import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useSettings, usePutSettings, MAXMIND_KEEP } from "../../api/hooks/useSettings";
import { useUploadLookupFile } from "../../api/hooks/useUploadLookupFile";
import { useToast } from "../Toast";
import { FormField, TextInput } from "./FormField";
import { Checkbox } from "./Checkbox";
import { Button } from "./Buttons";
import { ExpandableCard } from "./ExpandableCard";
import { handleDragOver, handleDragEnter, handleDragLeave } from "./CertificateForms";

// eslint-disable-next-line sonarjs/cognitive-complexity -- inherently complex settings form with multiple expandable cards and dirty tracking
export function LookupsSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useSettings();
  const putConfig = usePutSettings();
  const uploadFile = useUploadLookupFile();
  const { addToast } = useToast();

  const [autoDownload, setAutoDownload] = useState(false);
  const [accountId, setAccountId] = useState("");
  const [licenseKey, setLicenseKey] = useState("");
  const [initialized, setInitialized] = useState(false);

  const { toggle, isExpanded } = useExpandedCards({
    maxmind: true,
    geoip: false,
    asn: false,
  });

  if (data && !initialized) {
    setAutoDownload(data.lookup?.maxmind?.autoDownload ?? false);
    setAccountId("");
    setLicenseKey("");
    setInitialized(true);
  }

  const dirty =
    initialized &&
    data &&
    (autoDownload !== (data.lookup?.maxmind?.autoDownload ?? false) ||
      accountId !== "" ||
      licenseKey !== "");

  const handleSave = async () => {
    try {
      await putConfig.mutateAsync({
        lookup: {
          maxmind: {
            autoDownload,
            accountId: accountId || undefined,
            licenseKey: licenseKey || MAXMIND_KEEP,
          },
        },
      });
      setAccountId("");
      setLicenseKey("");
      addToast("Lookup configuration updated", "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to update lookup configuration", "error");
    }
  };

  const handleReset = () => {
    if (data) {
      setAutoDownload(data.lookup?.maxmind?.autoDownload ?? false);
      setAccountId("");
      setLicenseKey("");
    }
  };

  return (
    <div>
      {isLoading ? (
        <LoadingPlaceholder dark={dark} />
      ) : (
        <div className="flex flex-col gap-3">
          <ExpandableCard
            id="MaxMind Auto-Download"
            dark={dark}
            expanded={isExpanded("maxmind")}
            onToggle={() => toggle("maxmind")}
            monoTitle={false}
            typeBadge={autoDownload ? "enabled" : undefined}
            typeBadgeAccent={true}
          >
            <div className="flex flex-col gap-4">
              <Checkbox
                checked={autoDownload}
                onChange={setAutoDownload}
                label="Enable automatic database downloads"
                dark={dark}
              />

              <p
                className={`text-[0.8em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
              >
                When enabled, GeoLite2-City and GeoLite2-ASN databases are
                downloaded automatically and updated twice weekly (Tue/Fri at
                03:00). Requires a free{" "}
                <a
                  href="https://www.maxmind.com/en/geolite2/signup"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-copper hover:underline"
                >
                  MaxMind account
                </a>
                .
              </p>

              <FormField
                label="Account ID"
                description="Your MaxMind account ID (numeric)."
                dark={dark}
              >
                <TextInput
                  value={accountId}
                  onChange={setAccountId}
                  placeholder={
                    data?.lookup?.maxmind?.licenseConfigured
                      ? "(configured — leave blank to keep)"
                      : "123456"
                  }
                  dark={dark}
                  mono
                />
              </FormField>

              <FormField
                label="License Key"
                description="Your MaxMind license key."
                dark={dark}
              >
                <PasswordInput
                  value={licenseKey}
                  onChange={setLicenseKey}
                  placeholder={
                    data?.lookup?.maxmind?.licenseConfigured
                      ? "(configured — leave blank to keep)"
                      : "Enter license key"
                  }
                  dark={dark}
                />
              </FormField>

              {data?.lookup?.maxmind?.lastUpdate && (
                <div
                  className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Last updated:{" "}
                  {new Date(data.lookup.maxmind.lastUpdate).toLocaleString()}
                </div>
              )}
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="GeoIP"
            dark={dark}
            expanded={isExpanded("geoip")}
            onToggle={() => toggle("geoip")}
            monoTitle={false}
            typeBadge={autoDownload ? "auto" : undefined}
            typeBadgeAccent={autoDownload}
          >
            <div className="flex flex-col gap-4">
              <p
                className={`text-[0.8em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
              >
                Enriches IP addresses with country, city, and coordinates via{" "}
                <span className="font-mono">| lookup geoip</span>.
              </p>

              <MmdbDropZone
                dark={dark}
                label="GeoLite2-City / GeoIP2-City"
                expectedSuffix="City"
                uploadFile={uploadFile}
                addToast={addToast}
              />
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="ASN"
            dark={dark}
            expanded={isExpanded("asn")}
            onToggle={() => toggle("asn")}
            monoTitle={false}
            typeBadge={autoDownload ? "auto" : undefined}
            typeBadgeAccent={autoDownload}
          >
            <div className="flex flex-col gap-4">
              <p
                className={`text-[0.8em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
              >
                Enriches IP addresses with AS number and organization via{" "}
                <span className="font-mono">| lookup asn</span>.
              </p>

              <MmdbDropZone
                dark={dark}
                label="GeoLite2-ASN / GeoIP2-ISP"
                expectedSuffix="ASN"
                uploadFile={uploadFile}
                addToast={addToast}
              />
            </div>
          </ExpandableCard>

          <div className="flex gap-2 mt-2">
            <Button
              onClick={handleSave}
              disabled={!dirty || putConfig.isPending}
            >
              {putConfig.isPending ? "Saving..." : "Save"}
            </Button>
            {dirty && (
              <Button variant="ghost" onClick={handleReset} dark={dark}>
                Reset
              </Button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function MmdbDropZone({
  dark,
  label,
  expectedSuffix: _expectedSuffix,
  uploadFile,
  addToast,
}: Readonly<{
  dark: boolean;
  label: string;
  expectedSuffix: string;
  uploadFile: ReturnType<typeof useUploadLookupFile>;
  addToast: (msg: string, type: "info" | "error") => void;
}>) {
  const c = useThemeClass(dark);
  const [dragging, setDragging] = useState(false);
  const [uploadedName, setUploadedName] = useState<string | null>(null);

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.currentTarget.classList.remove("ring-2", "ring-copper");
    setDragging(false);

    const file = e.dataTransfer.files[0];
    if (!file) return;
    if (!file.name.endsWith(".mmdb")) {
      addToast("Only .mmdb files are accepted", "error");
      return;
    }

    uploadFile.mutate(file, {
      onSuccess: (result) => {
        setUploadedName(result.name);
        addToast(`Uploaded ${result.name} (${formatBytes(result.size)})`, "info");
      },
      onError: (err) => {
        addToast(err instanceof Error ? err.message : "Upload failed", "error");
      },
    });
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (!file.name.endsWith(".mmdb")) {
      addToast("Only .mmdb files are accepted", "error");
      return;
    }

    uploadFile.mutate(file, {
      onSuccess: (result) => {
        setUploadedName(result.name);
        addToast(`Uploaded ${result.name} (${formatBytes(result.size)})`, "info");
      },
      onError: (err) => {
        addToast(err instanceof Error ? err.message : "Upload failed", "error");
      },
    });
    // Reset so the same file can be re-uploaded.
    e.target.value = "";
  };

  return (
    <div
      role="button"
      tabIndex={0}
      onDragOver={handleDragOver}
      onDragEnter={(e) => { handleDragEnter(e); setDragging(true); }}
      onDragLeave={(e) => { handleDragLeave(e); if (!e.currentTarget.contains(e.relatedTarget as Node)) setDragging(false); }}
      onDrop={handleDrop}
      className={`relative flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed px-4 py-5 transition-all cursor-pointer ${
        dragging
          ? "ring-2 ring-copper border-copper"
          : c("border-ink-border hover:border-copper-dim", "border-light-border hover:border-copper")
      } ${c("bg-ink-surface/50", "bg-light-surface/50")}`}
      onClick={() => {
        const input = document.getElementById(`mmdb-upload-${label}`) as HTMLInputElement | null;
        input?.click();
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          const input = document.getElementById(`mmdb-upload-${label}`) as HTMLInputElement | null;
          input?.click();
        }
      }}
    >
      <input
        id={`mmdb-upload-${label}`}
        type="file"
        accept=".mmdb"
        className="hidden"
        onChange={handleFileInput}
      />

      {uploadFile.isPending ? (
        <span className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
          Uploading...
        </span>
      ) : (
        <>
          <span className={`text-[0.85em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}>
            Drop {label} .mmdb file here
          </span>
          <span className={`text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            or click to browse
          </span>
        </>
      )}

      {uploadedName && !uploadFile.isPending && (
        <span className={`text-[0.75em] mt-1 ${c("text-copper-dim", "text-copper")}`}>
          Uploaded: {uploadedName}
        </span>
      )}
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function PasswordInput({
  value,
  onChange,
  placeholder,
  dark,
}: Readonly<{
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  return (
    <input
      type="password"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      autoComplete="off"
      className={`px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors ${c(
        "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
        "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
      )}`}
    />
  );
}
