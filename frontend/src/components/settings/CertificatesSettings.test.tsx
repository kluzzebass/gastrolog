import { encode } from "../../api/glid";
import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { CertificatesSettings } from "./CertificatesSettings";

/** Create a distinct 16-byte Uint8Array test ID from a small number. */
function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const sampleCerts = {
  certificates: [
    { id: testId(1), name: "prod-cert" },
    { id: testId(2), name: "staging-cert" },
  ],
};

const sampleSettings = {
  tls: { defaultCert: encode(testId(1)) },
};

beforeEach(() => {
  m(mocks.systemClient, "listCertificates").mockClear();
  m(mocks.systemClient, "getCertificate").mockClear();
  m(mocks.systemClient, "putCertificate").mockClear();
  m(mocks.systemClient, "deleteCertificate").mockClear();
  m(mocks.systemClient, "getSettings").mockClear();
});

describe("CertificatesSettings", () => {
  test("renders empty state when no certificates", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    qc.setQueryData(["settings"], { tls: { defaultCert: "" } });

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/No certificates configured/)).toBeTruthy();
  });

  test("renders certificate list with names", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], sampleCerts);
    qc.setQueryData(["settings"], sampleSettings);

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("prod-cert")).toBeTruthy();
    expect(getByText("staging-cert")).toBeTruthy();
  });

  test("shows default badge on the default certificate", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], sampleCerts);
    qc.setQueryData(["settings"], sampleSettings);

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("default")).toBeTruthy();
  });

  test("shows add buttons for PEM and file modes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    qc.setQueryData(["settings"], { tls: { defaultCert: "" } });

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("Add pasted certificate")).toBeTruthy();
    expect(getByText("Add monitored files")).toBeTruthy();
  });

  test("hides add buttons when a cert is expanded", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], sampleCerts);
    qc.setQueryData(["settings"], sampleSettings);

    const { getByText, queryByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // Expand a cert
    fireEvent.click(getByText("prod-cert"));
    expect(queryByText("Add pasted certificate")).toBeNull();
    expect(queryByText("Add monitored files")).toBeNull();
  });

  test("opens PEM add form with textarea fields", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    qc.setQueryData(["settings"], { tls: { defaultCert: "" } });

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add pasted certificate"));
    expect(getByText("Certificate (PEM)")).toBeTruthy();
    expect(getByText("Private key (PEM)")).toBeTruthy();
  });

  test("opens files add form with path fields", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    qc.setQueryData(["settings"], { tls: { defaultCert: "" } });

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add monitored files"));
    expect(getByText("Certificate file path")).toBeTruthy();
    expect(getByText("Key file path")).toBeTruthy();
  });

  test("deletes certificate via confirm flow", async () => {
    m(mocks.systemClient, "deleteCertificate").mockResolvedValueOnce({});
    m(mocks.systemClient, "getCertificate").mockResolvedValueOnce({
      id: testId(1),
      name: "prod-cert",
      certPem: "",
      certFile: "",
      keyFile: "",
    });
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], sampleCerts);
    qc.setQueryData(["settings"], sampleSettings);

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("prod-cert"));
    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));

    await waitFor(() => {
      expect(m(mocks.systemClient, "deleteCertificate")).toHaveBeenCalledTimes(1);
    });
  });

  test("expanded cert shows Save button in footer", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], sampleCerts);
    qc.setQueryData(["settings"], sampleSettings);

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("prod-cert"));
    expect(getByText("Save")).toBeTruthy();
  });

  test("TLS description text is shown", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["certificates"], { certificates: [] });
    qc.setQueryData(["settings"], { tls: { defaultCert: "" } });

    const { getByText } = render(<CertificatesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/TLS certificates for the server/)).toBeTruthy();
  });
});
