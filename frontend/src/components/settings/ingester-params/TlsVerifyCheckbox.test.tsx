import { describe, test, expect } from "bun:test";
import { fireEvent } from "@testing-library/react";
import { installMockClients } from "../../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../../test/render";

installMockClients();

import { renderWritable } from "../../../testing/renderWritable";
import { DockerForm } from "./DockerForm";
import { KafkaForm } from "./KafkaForm";
import { MqttForm } from "./MqttForm";
import { SyslogForm } from "./SyslogForm";
import { TLS_VERIFY_LABEL, tlsVerifies } from "./TlsFields";
import type { SubFormProps } from "./types";

type Form = (props: SubFormProps) => React.ReactNode;

// Every ingester that dials a server renders the one shared checkbox, so a
// given `tls_verify` value reads the same on each form: checked verifies.
const dialingForms: Array<[string, Form, Record<string, string>]> = [
  ["docker", DockerForm, { tls: "true" }],
  ["kafka", KafkaForm, { tls: "true" }],
  ["mqtt", MqttForm, { tls: "true" }],
];

function renderForm(Form: Form, params: Record<string, string>) {
  const qc = createTestQueryClient();
  qc.setQueryData(["certificates"], { certificates: [] });
  let changed: Record<string, string> | undefined;
  const result = renderWritable(
    <Form params={params} onChange={(p) => { changed = p; }} dark defaults={{}} />,
    { wrapper: settingsWrapper(qc) },
  );
  const checkbox = result.getByText(TLS_VERIFY_LABEL).closest('[role="checkbox"]')!;
  return { ...result, checkbox, changed: () => changed };
}

describe("TLS server verification checkbox", () => {
  test("only the literal false turns verification off", () => {
    expect(tlsVerifies({})).toBe(true);
    expect(tlsVerifies({ tls_verify: "" })).toBe(true);
    expect(tlsVerifies({ tls_verify: "true" })).toBe(true);
    expect(tlsVerifies({ tls_verify: "false" })).toBe(false);
  });

  for (const [name, Form, base] of dialingForms) {
    test(`${name}: an unset tls_verify reads as checked, and unchecking writes false`, () => {
      const { checkbox, changed } = renderForm(Form, base);
      expect(checkbox.getAttribute("aria-checked")).toBe("true");
      fireEvent.click(checkbox);
      expect(changed()?.["tls_verify"]).toBe("false");
    });

    test(`${name}: tls_verify=false reads as unchecked, and checking clears it`, () => {
      const { checkbox, changed } = renderForm(Form, { ...base, tls_verify: "false" });
      expect(checkbox.getAttribute("aria-checked")).toBe("false");
      fireEvent.click(checkbox);
      expect(changed()?.["tls_verify"]).toBe("");
    });
  }

  test("a listener ingester has no server to verify and no such checkbox", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    const { queryByText } = renderWritable(
      <SyslogForm params={{ tls: "true" }} onChange={() => {}} dark defaults={{}} />,
      { wrapper: settingsWrapper(qc) },
    );
    expect(queryByText(TLS_VERIFY_LABEL)).toBeNull();
  });
});

describe("ingester form placeholders", () => {
  test("the MQTT client ID placeholder is empty: the description already states the auto-generated form", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    const { getByText, container } = renderWritable(
      <MqttForm params={{}} onChange={() => {}} dark defaults={{}} />,
      { wrapper: settingsWrapper(qc) },
    );
    const label = getByText("Client ID") as HTMLLabelElement;
    const input = container.querySelector(`#${CSS.escape(label.htmlFor)}`) as HTMLInputElement;
    expect(input.placeholder).toBe("");
    expect(getByText(/auto-generated as gastrolog-/)).toBeTruthy();
  });

  test("both syslog addresses say that empty disables the listener", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    const { getByText, container } = renderWritable(
      <SyslogForm params={{}} onChange={() => {}} dark defaults={{}} />,
      { wrapper: settingsWrapper(qc) },
    );
    expect(getByText("UDP listen address (leave empty to disable)")).toBeTruthy();
    expect(getByText("TCP listen address (leave empty to disable)")).toBeTruthy();
    for (const name of ["UDP Address", "TCP Address"]) {
      const label = getByText(name) as HTMLLabelElement;
      const input = container.querySelector(`#${CSS.escape(label.htmlFor)}`) as HTMLInputElement;
      expect(input.placeholder).toBe("");
    }
  });
});
