import { encode } from "../../../api/glid";
import { FormField, TextInput, SelectInput } from "../FormField";
import { Checkbox } from "../Checkbox";
import { useCertificates } from "../../../api/hooks/useCertificates";
import type { SubFormProps } from "./types";

export function RelpForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });

  const tlsEnabled = params["tls"] === "true";
  const { data: certsData } = useCertificates();
  const certs = certsData?.certificates ?? [];

  const certOptions = [
    { value: "", label: "(none)" },
    ...certs
      .map((c) => ({ value: c.name || encode(c.id), label: c.name || encode(c.id) }))
      .sort((a, b) => a.label.localeCompare(b.label)),
  ];

  return (
    <div className="flex flex-col gap-3">
      <FormField
        label="Listen Address"
        description="TCP address for RELP"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => set("addr", v)}
          placeholder={d["addr"] ?? ""}
          dark={dark}
          mono
          examples={[":2514"]}
        />
      </FormField>
      <Checkbox
        checked={tlsEnabled}
        onChange={(v) => set("tls", v ? "true" : "false")}
        label="Enable TLS"
        dark={dark}
      />
      {tlsEnabled && (
        <div className="flex flex-col gap-3">
          <FormField
            label="Certificate"
            description="Server certificate from the certificate manager"
            dark={dark}
          >
            <SelectInput
              value={params["tls_cert"] ?? ""}
              onChange={(v) => set("tls_cert", v)}
              options={certOptions}
              dark={dark}
            />
          </FormField>
          <FormField
            label="CA Certificate File"
            description="Path to CA certificate for client verification (mutual TLS)"
            dark={dark}
          >
            <TextInput
              value={params["tls_ca"] ?? ""}
              onChange={(v) => set("tls_ca", v)}
              dark={dark}
              mono
            />
          </FormField>
          <FormField
            label="Allowed Client CN"
            description="Wildcard pattern to match client certificate Common Name"
            dark={dark}
          >
            <TextInput
              value={params["tls_allowed_cn"] ?? ""}
              onChange={(v) => set("tls_allowed_cn", v)}
              dark={dark}
              mono
            />
          </FormField>
        </div>
      )}
    </div>
  );
}
