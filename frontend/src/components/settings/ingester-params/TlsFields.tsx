import { encode } from "../../../api/glid";
import { FormField, SelectInput, TextInput } from "../FormField";
import { Checkbox } from "../Checkbox";
import { useCertificates } from "../../../api/hooks/useCertificates";

/** useCertOptions lists stored certificates as select options. */
function useCertOptions() {
  const { data: certsData } = useCertificates();
  const certs = certsData?.certificates ?? [];
  return [
    { value: "", label: "(none)" },
    ...certs
      .map((c) => ({
        value: c.name || encode(c.id),
        label: c.name || encode(c.id),
      }))
      .sort((a, b) => a.label.localeCompare(b.label)),
  ];
}

interface TlsFieldsProps {
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}

/**
 * TLS fields for listener ingesters (syslog TCP, RELP, HTTP, OTLP, Fluent
 * Forward): a serving certificate from the certificate store, an optional
 * stored CA that demands and verifies client certificates (mutual TLS), and
 * an optional CN pattern narrowing which clients the CA's signature admits.
 */
export function TlsListenerFields({
  params,
  onChange,
  dark,
}: Readonly<TlsFieldsProps>) {
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });
  const tlsEnabled = params["tls"] === "true";
  const certOptions = useCertOptions();

  return (
    <>
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
            description="Server certificate from the certificate store"
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
            label="Client CA Certificate"
            description="Stored CA that client certificates must chain to (mutual TLS); empty accepts any client"
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
            label="Allowed Client CN"
            description="Wildcard pattern a client certificate's Common Name must match"
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
    </>
  );
}

/**
 * TLS fields for client ingesters that dial out (Kafka, MQTT): a stored CA
 * pinning the server's trust root, an optional stored client certificate for
 * brokers that demand one, and the unsafe verification opt-out.
 */
export function TlsClientFields({
  params,
  onChange,
  dark,
}: Readonly<TlsFieldsProps>) {
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });
  const tlsEnabled = params["tls"] === "true";
  const certOptions = useCertOptions();

  return (
    <>
      <Checkbox
        checked={tlsEnabled}
        onChange={(v) => set("tls", v ? "true" : "false")}
        label="Enable TLS"
        dark={dark}
      />
      {tlsEnabled && (
        <div className="flex flex-col gap-3">
          <FormField
            label="CA Certificate"
            description="Stored CA the server certificate must chain to; empty uses the system roots"
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
            description="Stored certificate presented to servers that demand one (mutual TLS)"
            dark={dark}
          >
            <SelectInput
              value={params["tls_cert"] ?? ""}
              onChange={(v) => set("tls_cert", v)}
              options={certOptions}
              dark={dark}
            />
          </FormField>
          <Checkbox
            checked={params["tls_verify"] === "false"}
            onChange={(v) => set("tls_verify", v ? "false" : "")}
            label="Disable server verification (unsafe — a CA certificate covers self-signed servers safely)"
            dark={dark}
          />
        </div>
      )}
    </>
  );
}
