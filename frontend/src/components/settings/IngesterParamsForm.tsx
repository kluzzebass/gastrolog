import { useState } from "react";
import { FormField, TextInput, NumberInput, SelectInput, ExampleValues } from "./FormField";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useCertificates } from "../../api/hooks/useConfig";
import { useTestIngester } from "../../api/hooks/useIngesters";
import { useIngesterDefaults } from "../../api/hooks/useIngesterDefaults";
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
  defaults: d,
}: Readonly<{
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
  defaults: Record<string, string>;
}>) {
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
  let totalWeight = 0;
  for (const f of ALL_FORMATS) {
    if (enabled.has(f.id)) {
      totalWeight += weights[f.id] ?? 1;
    }
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Format selection with weights */}
      <fieldset className="flex flex-col gap-1">
        <legend
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Log Formats
        </legend>
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
      </fieldset>

      {/* Timing */}
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Min Interval"
          description="Minimum delay between messages"
          dark={dark}
        >
          <TextInput
            value={get("minInterval")}
            onChange={(v) => set("minInterval", v)}
            placeholder={d["minInterval"] ?? ""}
            dark={dark}
            mono
            examples={["50ms", "100ms", "500ms"]}
          />
        </FormField>
        <FormField
          label="Max Interval"
          description="Maximum delay between messages"
          dark={dark}
        >
          <TextInput
            value={get("maxInterval")}
            onChange={(v) => set("maxInterval", v)}
            placeholder={d["maxInterval"] ?? ""}
            dark={dark}
            mono
            examples={["500ms", "1s", "5s"]}
          />
        </FormField>
      </div>

      {/* Cardinality */}
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Host Count"
          description="Distinct hosts to simulate"
          dark={dark}
        >
          <NumberInput
            value={get("hostCount")}
            onChange={(v) => set("hostCount", v)}
            placeholder={d["hostCount"] ?? ""}
            dark={dark}
            min={1}
            examples={["5", "10", "50"]}
          />
        </FormField>
        <FormField
          label="Service Count"
          description="Distinct services to simulate"
          dark={dark}
        >
          <NumberInput
            value={get("serviceCount")}
            onChange={(v) => set("serviceCount", v)}
            placeholder={d["serviceCount"] ?? ""}
            dark={dark}
            min={1}
            examples={["3", "5", "20"]}
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
  defaults: d,
}: Readonly<{
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
  defaults: Record<string, string>;
}>) {
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
          htmlFor="file-patterns"
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
          id="file-patterns"
          value={text}
          onChange={(e) => handleTextChange(e.target.value)}
          placeholder={"/var/log/app.log\n/var/log/**/*.log"}
          rows={3}
          className={`px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors resize-y ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
          )}`}
        />
        <ExampleValues
          examples={["/var/log/**/*.log", "/var/log/syslog", "/var/log/auth.log"]}
          value={text}
          onChange={handleTextChange}
          dark={dark}
        />
      </div>

      <FormField
        label="Poll Interval"
        description="How often to re-scan for new files and save bookmarks (0 to disable)"
        dark={dark}
      >
        <TextInput
          value={params["poll_interval"] ?? ""}
          onChange={(v) => onChange({ ...params, poll_interval: v })}
          placeholder={d["poll_interval"] ?? ""}
          dark={dark}
          mono
          examples={["30s", "1m", "5m"]}
        />
      </FormField>
    </div>
  );
}

function DockerForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<{
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
  defaults: Record<string, string>;
}>) {
  const c = useThemeClass(dark);
  const { data: certData } = useCertificates();
  const testIngester = useTestIngester();
  const [testResult, setTestResult] = useState<{
    success: boolean;
    message: string;
  } | null>(null);
  const certs = certData?.certificates ?? [];
  const certOptions = [
    { value: "", label: "(none)" },
    ...certs.map((c) => ({ value: c.id, label: c.name || c.id })),
  ];
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });

  return (
    <div className="flex flex-col gap-4">
      <FormField
        label="Docker Host"
        description="Docker daemon address"
        dark={dark}
      >
        <TextInput
          value={params["host"] ?? ""}
          onChange={(v) => set("host", v)}
          placeholder={d["host"] ?? ""}
          dark={dark}
          mono
          examples={["unix:///var/run/docker.sock", "tcp://localhost:2376"]}
        />
      </FormField>

      {/* Filter */}
      <FormField
        label="Container Filter"
        description="Boolean expression over container metadata. Leave empty to tail all containers."
        dark={dark}
      >
        <TextInput
          value={params["filter"] ?? ""}
          onChange={(v) => set("filter", v)}
          placeholder="image=nginx* AND label.env=prod"
          dark={dark}
          mono
          examples={["image=nginx*", "label.env=prod"]}
        />
      </FormField>

      {/* Streams & Polling */}
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Poll Interval"
          description="Backup container discovery interval"
          dark={dark}
        >
          <TextInput
            value={params["poll_interval"] ?? ""}
            onChange={(v) => set("poll_interval", v)}
            placeholder={d["poll_interval"] ?? ""}
            dark={dark}
            mono
            examples={["30s", "1m", "5m"]}
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
}: Readonly<IngesterParamsFormProps>) {
  const { data: allDefaults } = useIngesterDefaults();
  const d = allDefaults?.[ingesterType] ?? {};

  if (ingesterType === "chatterbox") {
    return <ChatterboxForm params={params} onChange={onChange} dark={dark} defaults={d} />;
  }

  if (ingesterType === "tail") {
    return <TailForm params={params} onChange={onChange} dark={dark} defaults={d} />;
  }

  if (ingesterType === "docker") {
    return <DockerForm params={params} onChange={onChange} dark={dark} defaults={d} />;
  }

  if (ingesterType === "otlp") {
    return (
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="HTTP Address"
          description="OTLP/HTTP listen address (POST /v1/logs)"
          dark={dark}
        >
          <TextInput
            value={params["http_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, http_addr: v })}
            placeholder={d["http_addr"] ?? ""}
            dark={dark}
            mono
            examples={[":4318"]}
          />
        </FormField>
        <FormField
          label="gRPC Address"
          description="OTLP/gRPC listen address"
          dark={dark}
        >
          <TextInput
            value={params["grpc_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, grpc_addr: v })}
            placeholder={d["grpc_addr"] ?? ""}
            dark={dark}
            mono
            examples={[":4317"]}
          />
        </FormField>
      </div>
    );
  }

  if (ingesterType === "fluentfwd") {
    return (
      <FormField
        label="Listen Address"
        description="TCP address for Fluent Forward protocol"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => onChange({ ...params, addr: v })}
          placeholder={d["addr"] ?? ""}
          dark={dark}
          mono
          examples={[":24224"]}
        />
      </FormField>
    );
  }

  if (ingesterType === "kafka") {
    const set = (key: string, value: string) =>
      onChange({ ...params, [key]: value });

    return (
      <div className="flex flex-col gap-3">
        <FormField
          label="Brokers"
          description="Comma-separated list of Kafka broker addresses (required)"
          dark={dark}
        >
          <TextInput
            value={params["brokers"] ?? ""}
            onChange={(v) => set("brokers", v)}
            placeholder="localhost:9092"
            dark={dark}
            mono
            examples={["localhost:9092", "broker1:9092,broker2:9092"]}
          />
        </FormField>
        <div className="grid grid-cols-2 gap-3">
          <FormField
            label="Topic"
            description="Kafka topic to consume (required)"
            dark={dark}
          >
            <TextInput
              value={params["topic"] ?? ""}
              onChange={(v) => set("topic", v)}
              placeholder="logs"
              dark={dark}
              mono
              examples={["logs", "app-logs"]}
            />
          </FormField>
          <FormField
            label="Consumer Group"
            description="Kafka consumer group ID"
            dark={dark}
          >
            <TextInput
              value={params["group"] ?? ""}
              onChange={(v) => set("group", v)}
              placeholder={d["group"] ?? ""}
              dark={dark}
              mono
              examples={["gastrolog"]}
            />
          </FormField>
        </div>
        <Checkbox
          checked={params["tls"] === "true"}
          onChange={(v) => set("tls", v ? "true" : "false")}
          label="Enable TLS"
          dark={dark}
        />
        <FormField
          label="SASL Mechanism"
          description="Authentication mechanism (leave empty to disable)"
          dark={dark}
        >
          <SelectInput
            value={params["sasl_mechanism"] ?? ""}
            onChange={(v) => set("sasl_mechanism", v)}
            options={[
              { value: "", label: "(none)" },
              { value: "plain", label: "PLAIN" },
              { value: "scram-sha-256", label: "SCRAM-SHA-256" },
              { value: "scram-sha-512", label: "SCRAM-SHA-512" },
            ]}
            dark={dark}
          />
        </FormField>
        {params["sasl_mechanism"] && (
          <div className="grid grid-cols-2 gap-3">
            <FormField label="SASL User" dark={dark}>
              <TextInput
                value={params["sasl_user"] ?? ""}
                onChange={(v) => set("sasl_user", v)}
                dark={dark}
                mono
              />
            </FormField>
            <FormField label="SASL Password" dark={dark}>
              <TextInput
                value={params["sasl_password"] ?? ""}
                onChange={(v) => set("sasl_password", v)}
                dark={dark}
                mono
              />
            </FormField>
          </div>
        )}
      </div>
    );
  }

  if (ingesterType === "http") {
    return (
      <FormField
        label="Listen Address"
        description="TCP address for HTTP/Loki Push API"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => onChange({ ...params, addr: v })}
          placeholder={d["addr"] ?? ""}
          dark={dark}
          mono
          examples={[":3100"]}
        />
      </FormField>
    );
  }

  if (ingesterType === "relp") {
    return (
      <FormField
        label="Listen Address"
        description="TCP address for RELP"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => onChange({ ...params, addr: v })}
          placeholder={d["addr"] ?? ""}
          dark={dark}
          mono
          examples={[":2514"]}
        />
      </FormField>
    );
  }

  if (ingesterType === "metrics") {
    return (
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="System Interval"
          description="How often to emit CPU/memory metrics"
          dark={dark}
        >
          <TextInput
            value={params["interval"] ?? ""}
            onChange={(v) => onChange({ ...params, interval: v })}
            placeholder={d["interval"] ?? ""}
            dark={dark}
            mono
            examples={["10s", "30s", "1m"]}
          />
        </FormField>
        <FormField
          label="Vault Interval"
          description="How often to emit vault stats metrics"
          dark={dark}
        >
          <TextInput
            value={params["vault_interval"] ?? ""}
            onChange={(v) => onChange({ ...params, vault_interval: v })}
            placeholder={d["vault_interval"] ?? ""}
            dark={dark}
            mono
            examples={["5s", "10s", "30s"]}
          />
        </FormField>
      </div>
    );
  }

  if (ingesterType === "syslog") {
    return (
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="UDP Address"
          description="UDP listen address"
          dark={dark}
        >
          <TextInput
            value={params["udp_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, udp_addr: v })}
            placeholder={d["udp_addr"] ?? ""}
            dark={dark}
            mono
            examples={[":514"]}
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
            examples={[":514"]}
          />
        </FormField>
      </div>
    );
  }

  return null;
}
