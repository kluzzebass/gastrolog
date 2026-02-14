import { useState } from "react";
import { FormField, TextInput, NumberInput, SelectInput } from "./FormField";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useCertificates, useTestIngester } from "../../api/hooks/useConfig";
import { Checkbox } from "./Checkbox";

interface IngesterParamsFormProps {
  ingesterType: string;
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}

const ALL_FORMATS = [
  { id: "plain", label: "Plain Text", description: "Unstructured log lines" },
  { id: "json", label: "JSON", description: "Structured JSON objects" },
  { id: "kv", label: "Key-Value", description: "key=value pairs" },
  { id: "access", label: "Access Log", description: "HTTP access log format" },
  { id: "syslog", label: "Syslog", description: "RFC 5424 syslog messages" },
  { id: "weird", label: "Weird", description: "Unusual / malformed entries" },
  {
    id: "multirecord",
    label: "Multi-Record",
    description: "Stack dumps, help output — each line as separate record",
  },
] as const;

function parseFormats(raw: string): Set<string> {
  if (!raw.trim()) return new Set(ALL_FORMATS.map((f) => f.id));
  return new Set(
    raw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
  );
}

function parseWeights(raw: string): Record<string, number> {
  const weights: Record<string, number> = {};
  if (!raw.trim()) return weights;
  for (const pair of raw.split(",")) {
    const eq = pair.indexOf("=");
    if (eq === -1) continue;
    const name = pair.slice(0, eq).trim();
    const val = parseInt(pair.slice(eq + 1).trim(), 10);
    if (name && !isNaN(val) && val > 0) weights[name] = val;
  }
  return weights;
}

function serializeFormats(enabled: Set<string>): string {
  // If all enabled, return empty (backend default is all)
  if (
    enabled.size === ALL_FORMATS.length &&
    ALL_FORMATS.every((f) => enabled.has(f.id))
  )
    return "";
  return ALL_FORMATS.filter((f) => enabled.has(f.id))
    .map((f) => f.id)
    .join(",");
}

function serializeWeights(
  weights: Record<string, number>,
  enabled: Set<string>,
): string {
  const parts: string[] = [];
  for (const f of ALL_FORMATS) {
    if (!enabled.has(f.id)) continue;
    const w = weights[f.id];
    if (w !== undefined && w !== 1) parts.push(`${f.id}=${w}`);
  }
  return parts.join(",");
}

function ChatterboxForm({
  params,
  onChange,
  dark,
}: {
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });
  const get = (key: string) => params[key] ?? "";

  const enabled = parseFormats(get("formats"));
  const weights = parseWeights(get("formatWeights"));

  const toggleFormat = (id: string) => {
    const next = new Set(enabled);
    if (next.has(id)) {
      if (next.size <= 1) return; // must have at least one format
      next.delete(id);
    } else {
      next.add(id);
    }
    const nextWeights = { ...weights };
    if (!next.has(id)) delete nextWeights[id];
    onChange({
      ...params,
      formats: serializeFormats(next),
      formatWeights: serializeWeights(nextWeights, next),
    });
  };

  const setWeight = (id: string, w: number) => {
    const clamped = Math.max(1, Math.round(w));
    const nextWeights = { ...weights, [id]: clamped };
    onChange({
      ...params,
      formatWeights: serializeWeights(nextWeights, enabled),
    });
  };

  // Compute total weight for percentage display
  const totalWeight = ALL_FORMATS.reduce((sum, f) => {
    if (!enabled.has(f.id)) return sum;
    return sum + (weights[f.id] ?? 1);
  }, 0);

  return (
    <div className="flex flex-col gap-4">
      {/* Format selection with weights */}
      <div className="flex flex-col gap-1">
        <label
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Log Formats
        </label>
        <p
          className={`text-[0.7em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Select which formats to generate and their relative weights. Higher
          weight means more frequent.
        </p>
        <div className="flex flex-col gap-1">
          {ALL_FORMATS.map((f) => {
            const isOn = enabled.has(f.id);
            const weight = weights[f.id] ?? 1;
            const pct =
              isOn && totalWeight > 0
                ? Math.round((weight / totalWeight) * 100)
                : 0;
            return (
              <div
                key={f.id}
                className={`flex items-center gap-3 px-3 py-2 rounded transition-colors ${c(
                  isOn ? "bg-ink-surface" : "bg-ink-well/50",
                  isOn ? "bg-light-surface" : "bg-light-well/50",
                )}`}
              >
                <Checkbox
                  checked={isOn}
                  onChange={() => toggleFormat(f.id)}
                  dark={dark}
                />

                {/* Label + description */}
                <div className="flex-1 min-w-0">
                  <span
                    className={`text-[0.85em] font-medium ${c(
                      isOn ? "text-text-bright" : "text-text-ghost",
                      isOn ? "text-light-text-bright" : "text-light-text-ghost",
                    )}`}
                  >
                    {f.label}
                  </span>
                  <span
                    className={`text-[0.75em] ml-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
                  >
                    {f.description}
                  </span>
                </div>

                {/* Weight input + percentage */}
                {isOn && (
                  <div className="flex items-center gap-1.5 shrink-0">
                    <input
                      type="text"
                      inputMode="numeric"
                      value={weight}
                      onChange={(e) => {
                        const v = e.target.value;
                        if (v === "" || /^\d+$/.test(v))
                          setWeight(f.id, parseInt(v, 10) || 1);
                      }}
                      className={`w-10 px-1 py-0.5 text-[0.8em] font-mono text-center border rounded focus:outline-none transition-colors [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none ${c(
                        "bg-ink-well border-ink-border text-text-bright focus:border-copper-dim",
                        "bg-light-well border-light-border text-light-text-bright focus:border-copper",
                      )}`}
                    />
                    <span
                      className={`text-[0.7em] w-8 text-right font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      {pct}%
                    </span>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Timing */}
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Min Interval"
          description="Minimum delay between messages (default: 100ms)"
          dark={dark}
        >
          <TextInput
            value={get("minInterval")}
            onChange={(v) => set("minInterval", v)}
            placeholder="100ms"
            dark={dark}
            mono
          />
        </FormField>
        <FormField
          label="Max Interval"
          description="Maximum delay between messages (default: 1s)"
          dark={dark}
        >
          <TextInput
            value={get("maxInterval")}
            onChange={(v) => set("maxInterval", v)}
            placeholder="1s"
            dark={dark}
            mono
          />
        </FormField>
      </div>

      {/* Cardinality */}
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Host Count"
          description="Distinct hosts to simulate (default: 10)"
          dark={dark}
        >
          <NumberInput
            value={get("hostCount")}
            onChange={(v) => set("hostCount", v)}
            placeholder="10"
            dark={dark}
            min={1}
          />
        </FormField>
        <FormField
          label="Service Count"
          description="Distinct services to simulate (default: 5)"
          dark={dark}
        >
          <NumberInput
            value={get("serviceCount")}
            onChange={(v) => set("serviceCount", v)}
            placeholder="5"
            dark={dark}
            min={1}
          />
        </FormField>
      </div>
    </div>
  );
}

function TailForm({
  params,
  onChange,
  dark,
}: {
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}) {
  const c = useThemeClass(dark);

  // Convert between JSON array and newline-separated text.
  let text = "";
  try {
    const raw = params["paths"];
    if (raw) text = (JSON.parse(raw) as string[]).join("\n");
  } catch {
    // invalid JSON — show raw value so user can fix it
    text = params["paths"] ?? "";
  }

  const handleTextChange = (value: string) => {
    const lines = value
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);
    onChange({ ...params, paths: lines.length > 0 ? JSON.stringify(lines) : "" });
  };

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-1">
        <label
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          File Patterns
        </label>
        <p
          className={`text-[0.7em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Glob patterns for files to tail, one per line. Supports ** for
          recursive matching.
        </p>
        <textarea
          value={text}
          onChange={(e) => handleTextChange(e.target.value)}
          placeholder={"/var/log/app.log\n/var/log/**/*.log"}
          rows={3}
          className={`px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors resize-y ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
          )}`}
        />
      </div>

      <FormField
        label="Poll Interval"
        description="How often to re-scan for new files and save bookmarks (default: 30s, 0s to disable)"
        dark={dark}
      >
        <TextInput
          value={params["poll_interval"] ?? ""}
          onChange={(v) => onChange({ ...params, poll_interval: v })}
          placeholder="30s"
          dark={dark}
          mono
        />
      </FormField>
    </div>
  );
}

function DockerForm({
  params,
  onChange,
  dark,
}: {
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  const { data: certData } = useCertificates();
  const testIngester = useTestIngester();
  const [testResult, setTestResult] = useState<{
    success: boolean;
    message: string;
  } | null>(null);
  const certNames = certData?.names ?? [];
  const certOptions = [
    { value: "", label: "(none)" },
    ...certNames.map((n: string) => ({ value: n, label: n })),
  ];
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });

  return (
    <div className="flex flex-col gap-4">
      <FormField
        label="Docker Host"
        description="Docker daemon address, e.g. unix:///var/run/docker.sock or tcp://docker-host:2376"
        dark={dark}
      >
        <TextInput
          value={params["host"] ?? ""}
          onChange={(v) => set("host", v)}
          placeholder="unix:///var/run/docker.sock"
          dark={dark}
          mono
        />
      </FormField>

      {/* Filters */}
      <div className="flex flex-col gap-1">
        <label
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Container Filters
        </label>
        <p
          className={`text-[0.7em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Only containers matching all specified filters will be tailed. Leave
          empty to tail all containers.
        </p>
        <div className="grid grid-cols-3 gap-3">
          <FormField label="Label Filter" dark={dark}>
            <TextInput
              value={params["label_filter"] ?? ""}
              onChange={(v) => set("label_filter", v)}
              placeholder="gastrolog.collect=true"
              dark={dark}
              mono
            />
          </FormField>
          <FormField label="Name Filter" dark={dark}>
            <TextInput
              value={params["name_filter"] ?? ""}
              onChange={(v) => set("name_filter", v)}
              placeholder="^web-.*"
              dark={dark}
              mono
            />
          </FormField>
          <FormField label="Image Filter" dark={dark}>
            <TextInput
              value={params["image_filter"] ?? ""}
              onChange={(v) => set("image_filter", v)}
              placeholder="nginx"
              dark={dark}
              mono
            />
          </FormField>
        </div>
      </div>

      {/* Streams & Polling */}
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Poll Interval"
          description="Backup container discovery interval (default: 30s)"
          dark={dark}
        >
          <TextInput
            value={params["poll_interval"] ?? ""}
            onChange={(v) => set("poll_interval", v)}
            placeholder="30s"
            dark={dark}
            mono
          />
        </FormField>
        <div className="flex items-end gap-4 pb-1">
          <Checkbox
            checked={params["stdout"] !== "false"}
            onChange={(v) => set("stdout", v ? "true" : "false")}
            label="Stdout"
            dark={dark}
          />
          <Checkbox
            checked={params["stderr"] !== "false"}
            onChange={(v) => set("stderr", v ? "true" : "false")}
            label="Stderr"
            dark={dark}
          />
        </div>
      </div>

      {/* TLS */}
      <div className="flex flex-col gap-1">
        <Checkbox
          checked={params["tls"] !== "false"}
          onChange={(v) => set("tls", v ? "true" : "false")}
          label="Enable TLS"
          dark={dark}
        />
        {params["tls"] !== "false" && (
          <div className="flex flex-col gap-2 mt-1">
            <p
              className={`text-[0.7em] ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              Select certificates from the Certificates tab. Leave empty to use
              system CAs without client authentication.
            </p>
            <div className="grid grid-cols-2 gap-3">
              <FormField
                label="CA Certificate"
                description="Verify the Docker daemon's server certificate"
                dark={dark}
              >
                <SelectInput
                  value={params["tls_ca"] ?? ""}
                  onChange={(v) => set("tls_ca", v)}
                  options={certOptions}
                  dark={dark}
                />
              </FormField>
              <FormField
                label="Client Certificate"
                description="Authenticate to the Docker daemon"
                dark={dark}
              >
                <SelectInput
                  value={params["tls_cert"] ?? ""}
                  onChange={(v) => set("tls_cert", v)}
                  options={certOptions}
                  dark={dark}
                />
              </FormField>
            </div>
            <Checkbox
              checked={params["tls_verify"] !== "false"}
              onChange={(v) => set("tls_verify", v ? "true" : "false")}
              label="Verify server certificate"
              dark={dark}
            />
          </div>
        )}
      </div>

      {/* Test Connection */}
      <div className="flex items-center gap-3">
        <button
          type="button"
          disabled={testIngester.isPending}
          onClick={() => {
            setTestResult(null);
            testIngester.mutate(
              { type: "docker", params },
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
          {testIngester.isPending ? "Testing..." : "Test Connection"}
        </button>
        {testResult && (
          <span
            className={`text-[0.8em] ${
              testResult.success
                ? c("text-green-400", "text-green-600")
                : c("text-red-400", "text-red-600")
            }`}
          >
            {testResult.message}
          </span>
        )}
      </div>
    </div>
  );
}

export function IngesterParamsForm({
  ingesterType,
  params,
  onChange,
  dark,
}: IngesterParamsFormProps) {
  if (ingesterType === "chatterbox") {
    return <ChatterboxForm params={params} onChange={onChange} dark={dark} />;
  }

  if (ingesterType === "tail") {
    return <TailForm params={params} onChange={onChange} dark={dark} />;
  }

  if (ingesterType === "docker") {
    return <DockerForm params={params} onChange={onChange} dark={dark} />;
  }

  if (ingesterType === "http") {
    return (
      <FormField
        label="Listen Address"
        description="TCP address for HTTP/Loki Push API (default: :3100)"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => onChange({ ...params, addr: v })}
          placeholder=":3100"
          dark={dark}
          mono
        />
      </FormField>
    );
  }

  if (ingesterType === "relp") {
    return (
      <FormField
        label="Listen Address"
        description="TCP address for RELP (default: :2514)"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => onChange({ ...params, addr: v })}
          placeholder=":2514"
          dark={dark}
          mono
        />
      </FormField>
    );
  }

  if (ingesterType === "syslog") {
    return (
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="UDP Address"
          description="UDP listen address (default: :514)"
          dark={dark}
        >
          <TextInput
            value={params["udp_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, udp_addr: v })}
            placeholder=":514"
            dark={dark}
            mono
          />
        </FormField>
        <FormField
          label="TCP Address"
          description="TCP listen address (leave empty to disable)"
          dark={dark}
        >
          <TextInput
            value={params["tcp_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, tcp_addr: v })}
            placeholder=""
            dark={dark}
            mono
          />
        </FormField>
      </div>
    );
  }

  return null;
}
