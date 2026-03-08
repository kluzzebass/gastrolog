import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useSettings, usePutSettings, useTestHTTPLookup, MAXMIND_KEEP } from "../../api/hooks/useSettings";
import { useConfig } from "../../api/hooks/useConfig";
import { useUploadManagedFile } from "../../api/hooks/useUploadManagedFile";
import { useToast } from "../Toast";
import { FormField, TextInput, ParamsEditor } from "./FormField";
import { Checkbox } from "./Checkbox";
import { Button } from "./Buttons";
import { ExpandableCard } from "./ExpandableCard";
import { handleDragOver, handleDragEnter, handleDragLeave } from "./CertificateForms";
import type { ManagedFileInfo, HTTPLookupEntry } from "../../api/gen/gastrolog/v1/config_pb";

interface HTTPLookupParamDraft {
  name: string;
  description: string;
}

interface HTTPLookupDraft {
  name: string;
  urlTemplate: string;
  headers: Record<string, string>;
  responsePaths: string[];
  parameters: HTTPLookupParamDraft[];
  timeout: string;
  cacheTtl: string;
  cacheSize: number;
}

// eslint-disable-next-line sonarjs/cognitive-complexity -- inherently complex settings form with multiple expandable cards and dirty tracking
export function LookupsSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useSettings();
  const { data: config } = useConfig();
  const putConfig = usePutSettings();
  const uploadFile = useUploadManagedFile();
  const { addToast } = useToast();

  const managedFiles = config?.managedFiles ?? [];
  const geoipFile = managedFiles.find((f) => f.name.includes("City"));
  const asnFile = managedFiles.find((f) => f.name.includes("ASN") || f.name.includes("ISP"));

  const testLookup = useTestHTTPLookup();
  const [autoDownload, setAutoDownload] = useState(false);
  const [accountId, setAccountId] = useState("");
  const [licenseKey, setLicenseKey] = useState("");
  const [httpLookups, setHttpLookups] = useState<HTTPLookupDraft[]>([]);
  const [initialized, setInitialized] = useState(false);
  const [testValues, setTestValues] = useState<Record<number, Record<string, string>>>({});
  const [testResults, setTestResults] = useState<Record<number, { success: boolean; error?: string; results?: { label: string; value: string; fields: Record<string, string> }[] }>>({});

  const { toggle, isExpanded } = useExpandedCards({
    maxmind: true,
    geoip: false,
    asn: false,
  });

  if (data && !initialized) {
    setAutoDownload(data.lookup?.maxmind?.autoDownload ?? false);
    setAccountId("");
    setLicenseKey("");
    setHttpLookups(
      (data.lookup?.httpLookups ?? []).map((h) => ({
        name: h.name,
        urlTemplate: h.urlTemplate,
        headers: { ...h.headers },
        responsePaths: [...(h.responsePaths ?? [])],
        parameters: (h.parameters ?? []).map((p) => ({ name: p.name, description: p.description })),
        timeout: h.timeout,
        cacheTtl: h.cacheTtl,
        cacheSize: h.cacheSize,
      })),
    );
    setInitialized(true);
  }

  const httpLookupsDirty = initialized && data && !httpLookupsEqual(httpLookups, data.lookup?.httpLookups ?? []);

  const dirty =
    initialized &&
    data &&
    (autoDownload !== (data.lookup?.maxmind?.autoDownload ?? false) ||
      accountId !== "" ||
      licenseKey !== "" ||
      httpLookupsDirty);

  const handleSave = async () => {
    try {
      await putConfig.mutateAsync({
        lookup: {
          maxmind: {
            autoDownload,
            accountId: accountId || undefined,
            licenseKey: licenseKey || MAXMIND_KEEP,
          },
          httpLookups: httpLookups
            .filter((h) => h.name && h.urlTemplate)
            .map((h) => ({
              name: h.name,
              urlTemplate: h.urlTemplate,
              headers: h.headers,
              responsePaths: h.responsePaths.filter(Boolean),
              parameters: h.parameters.filter((p) => p.name),
              timeout: h.timeout || undefined,
              cacheTtl: h.cacheTtl || undefined,
              cacheSize: h.cacheSize || undefined,
            })),
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
      setHttpLookups(
        (data.lookup?.httpLookups ?? []).map((h) => ({
          name: h.name,
          urlTemplate: h.urlTemplate,
          headers: { ...h.headers },
          responsePaths: [...(h.responsePaths ?? [])],
          parameters: (h.parameters ?? []).map((p) => ({ name: p.name, description: p.description })),
          timeout: h.timeout,
          cacheTtl: h.cacheTtl,
          cacheSize: h.cacheSize,
        })),
      );
    }
  };

  const updateHttpLookup = (index: number, update: Partial<HTTPLookupDraft>) => {
    setHttpLookups((prev) => prev.map((h, i) => (i === index ? { ...h, ...update } : h)));
  };

  const removeHttpLookup = (index: number) => {
    setHttpLookups((prev) => prev.filter((_, i) => i !== index));
  };

  const addHttpLookup = () => {
    setHttpLookups((prev) => [
      ...prev,
      { name: "", urlTemplate: "", headers: {}, responsePaths: [], parameters: [], timeout: "", cacheTtl: "", cacheSize: 0 },
    ]);
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
                      : ""
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
                      : ""
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
            typeBadge={geoipFile ? "active" : autoDownload ? "auto" : undefined}
            typeBadgeAccent={!!geoipFile || autoDownload}
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
                currentFile={geoipFile}
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
            typeBadge={asnFile ? "active" : autoDownload ? "auto" : undefined}
            typeBadgeAccent={!!asnFile || autoDownload}
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
                currentFile={asnFile}
                uploadFile={uploadFile}
                addToast={addToast}
              />
            </div>
          </ExpandableCard>

          {httpLookups.map((h, i) => (
            <ExpandableCard
              key={i}
              id={h.name || `HTTP Lookup ${i + 1}`}
              dark={dark}
              expanded={isExpanded(`http-${i}`)}
              onToggle={() => toggle(`http-${i}`)}
              monoTitle={false}
              typeBadge="http"
              typeBadgeAccent={!!(h.name && h.urlTemplate)}
              headerRight={
                <button
                  onClick={(e) => { e.stopPropagation(); removeHttpLookup(i); }}
                  className={`px-1.5 py-0.5 text-[0.75em] rounded transition-colors ${c(
                    "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
                    "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
                  )}`}
                >
                  Remove
                </button>
              }
            >
              <div className="flex flex-col gap-3">
                <FormField label="Name" description="Registry name used in queries, e.g. | lookup users" dark={dark}>
                  <TextInput value={h.name} onChange={(v) => updateHttpLookup(i, { name: v })} placeholder="" dark={dark} mono />
                </FormField>
                <FormField label="URL Template" description="Use {param} placeholders matching parameter names below. Fields map positionally in queries." dark={dark}>
                  <TextInput value={h.urlTemplate} onChange={(v) => updateHttpLookup(i, { urlTemplate: v })} placeholder="" dark={dark} mono />
                </FormField>
                <FormField label="Response Paths" description="JSONPath expressions to extract target objects. Results are merged." dark={dark}>
                  <StringListEditor
                    values={h.responsePaths}
                    onChange={(v) => updateHttpLookup(i, { responsePaths: v })}
                    placeholder="$.data.user"
                    dark={dark}
                  />
                </FormField>
                <FormField label="Parameters" description="Ordered list of URL template parameters. Names become {name} placeholders. Fields map positionally in queries." dark={dark}>
                  <ParameterListEditor
                    values={h.parameters}
                    onChange={(v) => updateHttpLookup(i, { parameters: v })}
                    dark={dark}
                  />
                </FormField>
                <FormField label="Headers" description="Custom HTTP headers (e.g. Authorization)." dark={dark}>
                  <ParamsEditor params={h.headers} onChange={(v) => updateHttpLookup(i, { headers: v })} dark={dark} />
                </FormField>
                <div className="grid grid-cols-3 gap-3">
                  <FormField label="Timeout" dark={dark}>
                    <TextInput value={h.timeout} onChange={(v) => updateHttpLookup(i, { timeout: v })} placeholder="5s" dark={dark} mono />
                  </FormField>
                  <FormField label="Cache TTL" dark={dark}>
                    <TextInput value={h.cacheTtl} onChange={(v) => updateHttpLookup(i, { cacheTtl: v })} placeholder="5m" dark={dark} mono />
                  </FormField>
                  <FormField label="Cache Size" dark={dark}>
                    <TextInput
                      value={h.cacheSize ? String(h.cacheSize) : ""}
                      onChange={(v) => updateHttpLookup(i, { cacheSize: parseInt(v) || 0 })}
                      placeholder="10000"
                      dark={dark}
                      mono
                    />
                  </FormField>
                </div>

                <div className={`mt-1 pt-3 border-t ${c("border-ink-border", "border-light-border")}`}>
                  {h.parameters.length > 0 && (
                    <FormField label="Test Values" dark={dark}>
                      <div className="flex flex-col gap-1.5">
                        {h.parameters.filter((p) => p.name).map((p) => (
                          <div key={p.name} className="flex gap-1.5 items-center">
                            <span className={`w-32 text-[0.85em] font-mono shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}>
                              {p.name}
                            </span>
                            <input
                              type="text"
                              value={testValues[i]?.[p.name] ?? ""}
                              onChange={(e) => setTestValues((prev) => ({ ...prev, [i]: { ...prev[i], [p.name]: e.target.value } }))}
                              placeholder={p.description || p.name}
                              className={`flex-1 px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none ${c(
                                "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                                "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
                              )}`}
                            />
                          </div>
                        ))}
                      </div>
                    </FormField>
                  )}
                  <div className="mt-2">
                    <Button
                      variant="ghost"
                      dark={dark}
                      disabled={!h.urlTemplate || testLookup.isPending}
                      onClick={() => {
                        setTestResults((prev) => ({ ...prev, [i]: undefined as never }));
                        testLookup.mutate(
                          {
                            config: {
                              name: h.name,
                              urlTemplate: h.urlTemplate,
                              headers: h.headers,
                              responsePaths: h.responsePaths.filter(Boolean),
                              timeout: h.timeout || undefined,
                              cacheTtl: h.cacheTtl || undefined,
                              cacheSize: h.cacheSize || undefined,
                            },
                            values: testValues[i] ?? {},
                          },
                          {
                            onSuccess: (res) => {
                              setTestResults((prev) => ({
                                ...prev,
                                [i]: { success: res.success, error: res.error, results: res.results },
                              }));
                            },
                            onError: (err) => {
                              setTestResults((prev) => ({
                                ...prev,
                                [i]: { success: false, error: err instanceof Error ? err.message : "Test failed" },
                              }));
                            },
                          },
                        );
                      }}
                    >
                      {testLookup.isPending ? "Testing..." : "Test"}
                    </Button>
                  </div>
                  {testResults[i] && (
                    <div className={`mt-2 rounded px-3 py-2 text-[0.8em] ${
                      testResults[i]!.success
                        ? c("bg-severity-ok/10 text-severity-ok", "bg-severity-ok/10 text-severity-ok")
                        : c("bg-severity-error/10 text-severity-error", "bg-severity-error/10 text-severity-error")
                    }`}>
                      {testResults[i]!.success ? (() => {
                        const fields = testResults[i]!.results?.[0]?.fields ?? {};
                        const entries = Object.entries(fields);
                        return entries.length > 0 ? (
                          <div className={`font-mono text-[0.9em] ${c("text-text-bright", "text-light-text-bright")}`}>
                            {entries.map(([k, v]) => (
                              <div key={k}><span className="text-copper">{k}</span> = {v}</div>
                            ))}
                          </div>
                        ) : (
                          <span>no results</span>
                        );
                      })() : (
                        <span>{testResults[i]!.error}</span>
                      )}
                    </div>
                  )}
                </div>
              </div>
            </ExpandableCard>
          ))}

          <button
            onClick={addHttpLookup}
            className={`flex items-center justify-center gap-1.5 rounded-lg border-2 border-dashed px-4 py-3 text-[0.85em] transition-colors ${c(
              "border-ink-border text-text-muted hover:border-copper-dim hover:text-copper",
              "border-light-border text-light-text-muted hover:border-copper hover:text-copper",
            )}`}
          >
            + Add HTTP Lookup
          </button>

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
  currentFile,
  uploadFile,
  addToast,
}: Readonly<{
  dark: boolean;
  label: string;
  currentFile?: ManagedFileInfo;
  uploadFile: ReturnType<typeof useUploadManagedFile>;
  addToast: (msg: string, type: "info" | "error") => void;
}>) {
  const c = useThemeClass(dark);
  const [dragging, setDragging] = useState(false);

  const doUpload = (file: File) => {
    if (!file.name.endsWith(".mmdb")) {
      addToast("Only .mmdb files are accepted", "error");
      return;
    }
    uploadFile.mutate(file, {
      onSuccess: (result) => {
        addToast(`Uploaded ${result.name} (${formatBytes(result.size)})`, "info");
      },
      onError: (err) => {
        addToast(err instanceof Error ? err.message : "Upload failed", "error");
      },
    });
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) doUpload(file);
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) doUpload(file);
    e.target.value = "";
  };

  return (
    <div className="flex flex-col gap-2">
      {currentFile && (
        <div className={`flex items-center gap-2 px-3 py-2 rounded text-[0.8em] ${c("bg-ink-surface", "bg-light-surface")}`}>
          <span className={`font-mono ${c("text-text-bright", "text-light-text-bright")}`}>
            {currentFile.name}
          </span>
          <span className={c("text-text-ghost", "text-light-text-ghost")}>
            {formatBytes(Number(currentFile.size))}
          </span>
          {currentFile.uploadedAt && (
            <span className={c("text-text-ghost", "text-light-text-ghost")}>
              &middot; {new Date(currentFile.uploadedAt).toLocaleDateString()}
            </span>
          )}
        </div>
      )}

      <div
        role="button"
        tabIndex={0}
        onDragOver={handleDragOver}
        onDragEnter={(e) => { handleDragEnter(e); setDragging(true); }}
        onDragLeave={(e) => { handleDragLeave(e); if (!e.currentTarget.contains(e.relatedTarget as Node)) setDragging(false); }}
        onDrop={handleDrop}
        className={`relative flex flex-col items-center justify-center gap-1 rounded-lg border-2 border-dashed px-4 py-4 transition-all cursor-pointer ${
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
              {currentFile ? "Replace" : "Drop"} {label} .mmdb
            </span>
            <span className={`text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
              or click to browse
            </span>
          </>
        )}
      </div>
    </div>
  );
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function StringListEditor({
  values,
  onChange,
  placeholder,
  dark,
}: Readonly<{
  values: string[];
  onChange: (v: string[]) => void;
  placeholder: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const [draft, setDraft] = useState("");

  const handleAdd = () => {
    if (!draft.trim()) return;
    onChange([...values, draft.trim()]);
    setDraft("");
  };

  return (
    <div className="flex flex-col gap-1.5">
      {values.map((v, i) => (
        <div key={i} className="flex gap-1.5 items-center">
          <input
            type="text"
            value={v}
            onChange={(e) => {
              const next = [...values];
              next[i] = e.target.value;
              onChange(next);
            }}
            className={`flex-1 px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none ${c(
              "bg-ink-surface border-ink-border text-text-bright focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright focus:border-copper",
            )}`}
          />
          <button
            onClick={() => onChange(values.filter((_, j) => j !== i))}
            className={`px-2 py-1.5 text-[0.8em] rounded border transition-colors ${c(
              "border-ink-border text-text-ghost hover:text-severity-error hover:border-severity-error hover:bg-ink-hover",
              "border-light-border text-light-text-ghost hover:text-severity-error hover:border-severity-error hover:bg-light-hover",
            )}`}
          >
            Remove
          </button>
        </div>
      ))}
      <div className="flex gap-1.5 items-center">
        <input
          type="text"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder={placeholder}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
          className={`flex-1 px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
          )}`}
        />
        <button
          onClick={handleAdd}
          className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
            "border-ink-border text-text-muted hover:text-copper hover:border-copper-dim hover:bg-ink-hover",
            "border-light-border text-light-text-muted hover:text-copper hover:border-copper hover:bg-light-hover",
          )}`}
        >
          Add
        </button>
      </div>
    </div>
  );
}

function ParameterListEditor({
  values,
  onChange,
  dark,
}: Readonly<{
  values: HTTPLookupParamDraft[];
  onChange: (v: HTTPLookupParamDraft[]) => void;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);

  const swap = (a: number, b: number) => {
    const next = [...values];
    [next[a], next[b]] = [next[b]!, next[a]!];
    onChange(next);
  };

  return (
    <div className="flex flex-col gap-1.5">
      {values.map((p, i) => (
        <div key={i} className="flex gap-1.5 items-center">
          <div className="flex flex-col gap-0.5">
            <button
              disabled={i === 0}
              onClick={() => swap(i, i - 1)}
              className={`px-1 py-0 text-[0.7em] leading-none rounded transition-colors disabled:opacity-20 ${c(
                "text-text-ghost hover:text-copper",
                "text-light-text-ghost hover:text-copper",
              )}`}
            >
              ▲
            </button>
            <button
              disabled={i === values.length - 1}
              onClick={() => swap(i, i + 1)}
              className={`px-1 py-0 text-[0.7em] leading-none rounded transition-colors disabled:opacity-20 ${c(
                "text-text-ghost hover:text-copper",
                "text-light-text-ghost hover:text-copper",
              )}`}
            >
              ▼
            </button>
          </div>
          <input
            type="text"
            value={p.name}
            onChange={(e) => {
              const next = [...values];
              next[i] = { ...next[i]!, name: e.target.value };
              onChange(next);
            }}
            placeholder="name"
            className={`w-32 px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none ${c(
              "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
            )}`}
          />
          <input
            type="text"
            value={p.description}
            onChange={(e) => {
              const next = [...values];
              next[i] = { ...next[i]!, description: e.target.value };
              onChange(next);
            }}
            placeholder="description"
            className={`flex-1 px-2.5 py-1.5 text-[0.85em] border rounded focus:outline-none ${c(
              "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
            )}`}
          />
          <button
            onClick={() => onChange(values.filter((_, j) => j !== i))}
            className={`px-2 py-1.5 text-[0.8em] rounded border transition-colors ${c(
              "border-ink-border text-text-ghost hover:text-severity-error hover:border-severity-error hover:bg-ink-hover",
              "border-light-border text-light-text-ghost hover:text-severity-error hover:border-severity-error hover:bg-light-hover",
            )}`}
          >
            Remove
          </button>
        </div>
      ))}
      <button
        onClick={() => onChange([...values, { name: "", description: "" }])}
        className={`self-start px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
          "border-ink-border text-text-muted hover:text-copper hover:border-copper-dim hover:bg-ink-hover",
          "border-light-border text-light-text-muted hover:text-copper hover:border-copper hover:bg-light-hover",
        )}`}
      >
        + Add Parameter
      </button>
    </div>
  );
}

function httpLookupsEqual(drafts: HTTPLookupDraft[], saved: HTTPLookupEntry[]): boolean {
  if (drafts.length !== saved.length) return false;
  for (let i = 0; i < drafts.length; i++) {
    const d = drafts[i]!;
    const s = saved[i]!;
    if (
      d.name !== s.name ||
      d.urlTemplate !== s.urlTemplate ||
      !arraysEqual(d.responsePaths, s.responsePaths) ||
      d.timeout !== s.timeout ||
      d.cacheTtl !== s.cacheTtl ||
      d.cacheSize !== s.cacheSize
    ) return false;
    const dParams = d.parameters;
    const sParams = s.parameters ?? [];
    if (dParams.length !== sParams.length) return false;
    for (let j = 0; j < dParams.length; j++) {
      if (dParams[j]!.name !== sParams[j]!.name || dParams[j]!.description !== sParams[j]!.description) return false;
    }
    const dKeys = Object.keys(d.headers);
    const sKeys = Object.keys(s.headers);
    if (dKeys.length !== sKeys.length) return false;
    for (const k of dKeys) {
      if (d.headers[k] !== s.headers[k]) return false;
    }
  }
  return true;
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
