import { useState } from "react";
import { FormField, TextInput, SelectInput } from "../FormField";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { useCertificates } from "../../../api/hooks/useCertificates";
import { useTestIngester } from "../../../api/hooks/useIngesters";
import { Checkbox } from "../Checkbox";
import type { SubFormProps } from "./types";

export function DockerForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
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
