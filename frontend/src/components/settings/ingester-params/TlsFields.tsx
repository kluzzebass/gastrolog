import { FormField, TextInput } from "../FormField";
import { Checkbox } from "../Checkbox";
import { EligibilitySelect } from "../EligibilitySelect";
import { useCertOptions } from "./useCertOptions";
import { tlsVerifies } from "./validation";

export const CERTIFICATES_HELP_REF = "certificates#adding-a-certificate";
export const NO_CERTIFICATES_MESSAGE = "No certificates stored — add one in the Certificates tab.";

interface TlsFieldsProps {
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}

export const TLS_VERIFY_LABEL =
  "Verify server certificate (turning this off is unsafe — a stored CA certificate covers self-signed servers)";

/**
 * The one server-verification checkbox every dialing ingester shares, so the
 * same `tls_verify` param never reads with opposite polarity on two forms.
 */
export function TlsVerifyCheckbox({
  params,
  onChange,
  dark,
}: Readonly<TlsFieldsProps>) {
  return (
    <Checkbox
      checked={tlsVerifies(params)}
      onChange={(v) => onChange({ ...params, tls_verify: v ? "" : "false" })}
      label={TLS_VERIFY_LABEL}
      dark={dark}
    />
  );
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
  const hasClientCa = (params["tls_ca"] ?? "") !== "";

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
            <EligibilitySelect
              value={params["tls_cert"] ?? ""}
              onChange={(v) => set("tls_cert", v)}
              options={certOptions}
              placeholder="(none)"
              emptyMessage={NO_CERTIFICATES_MESSAGE}
              helpRef={CERTIFICATES_HELP_REF}
              dark={dark}
            />
          </FormField>
          <FormField
            label="Client CA Certificate"
            description="Stored CA that client certificates must chain to (mutual TLS); empty accepts any client"
            dark={dark}
          >
            <EligibilitySelect
              value={params["tls_ca"] ?? ""}
              onChange={(v) => set("tls_ca", v)}
              options={certOptions}
              placeholder="(none)"
              emptyMessage={NO_CERTIFICATES_MESSAGE}
              helpRef={CERTIFICATES_HELP_REF}
              dark={dark}
            />
          </FormField>
          <FormField
            label="Allowed Client CN"
            description="Wildcard pattern a client certificate's Common Name must match"
            disabledReason={hasClientCa ? undefined : "Requires a client CA certificate — without one no client certificate is requested."}
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
 * brokers that demand one, and the shared server-verification checkbox.
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
            <EligibilitySelect
              value={params["tls_ca"] ?? ""}
              onChange={(v) => set("tls_ca", v)}
              options={certOptions}
              placeholder="(none)"
              emptyMessage={NO_CERTIFICATES_MESSAGE}
              helpRef={CERTIFICATES_HELP_REF}
              dark={dark}
            />
          </FormField>
          <FormField
            label="Client Certificate"
            description="Stored certificate presented to servers that demand one (mutual TLS)"
            dark={dark}
          >
            <EligibilitySelect
              value={params["tls_cert"] ?? ""}
              onChange={(v) => set("tls_cert", v)}
              options={certOptions}
              placeholder="(none)"
              emptyMessage={NO_CERTIFICATES_MESSAGE}
              helpRef={CERTIFICATES_HELP_REF}
              dark={dark}
            />
          </FormField>
          <TlsVerifyCheckbox params={params} onChange={onChange} dark={dark} />
        </div>
      )}
    </>
  );
}
