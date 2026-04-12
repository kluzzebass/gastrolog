import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { ServiceSettings } from "./ServiceSettings";

const sampleSettings = {
  auth: {
    tokenDuration: "15m",
    refreshTokenDuration: "168h",
    jwtSecretConfigured: true,
    passwordPolicy: {
      minLength: 8,
      requireMixedCase: false,
      requireDigit: true,
      requireSpecial: false,
      maxConsecutiveRepeats: 0,
      forbidAnimalNoise: false,
    },
  },
  scheduler: { maxConcurrentJobs: 4 },
  query: { timeout: "30s", maxFollowDuration: "4h", maxResultCount: 10000 },
  tls: { defaultCert: "", enabled: false, httpToHttpsRedirect: false, httpsPort: "" },
  cluster: { broadcastInterval: "5s" },
};

beforeEach(() => {
  m(mocks.systemClient, "getSettings").mockClear();
  m(mocks.systemClient, "putSettings").mockClear();
  m(mocks.systemClient, "listCertificates").mockClear();
});

describe("ServiceSettings", () => {
  test("renders all section cards", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("Authentication")).toBeTruthy();
    expect(getByText("Password Policy")).toBeTruthy();
    expect(getByText("Scheduler")).toBeTruthy();
    expect(getByText("Query")).toBeTruthy();
    expect(getByText("TLS")).toBeTruthy();
    expect(getByText("Broadcasting")).toBeTruthy();
  });

  test("authentication section expanded by default with field labels", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("Token Duration")).toBeTruthy();
    expect(getByText("Refresh Token Duration")).toBeTruthy();
    expect(getByText("JWT Secret")).toBeTruthy();
  });

  test("initializes form values from settings data", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByDisplayValue } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByDisplayValue("15m")).toBeTruthy();
    expect(getByDisplayValue("168h")).toBeTruthy();
  });

  test("password policy fields visible when expanded", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // Expand password policy
    fireEvent.click(getByText("Password Policy"));
    expect(getByText("Minimum length")).toBeTruthy();
    expect(getByText("Max consecutive repeats")).toBeTruthy();
    expect(getByText(/Require mixed case/)).toBeTruthy();
    expect(getByText(/Require digit/)).toBeTruthy();
    expect(getByText(/Require special character/)).toBeTruthy();
    expect(getByText(/Forbid animal noises/)).toBeTruthy();
  });

  test("scheduler section shows max concurrent jobs", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Scheduler"));
    expect(getByText("Max Concurrent Jobs")).toBeTruthy();
  });

  test("query section shows timeout and limits", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Query"));
    expect(getByText("Query Timeout")).toBeTruthy();
    expect(getByText("Max Follow Duration")).toBeTruthy();
    expect(getByText("Max Result Count")).toBeTruthy();
  });

  test("TLS section shows certificate dropdown", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], {
      certificates: [{ id: "c1", name: "my-cert" }],
    });

    const { getByText } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("TLS"));
    expect(getByText("Default certificate")).toBeTruthy();
    expect(getByText("my-cert")).toBeTruthy();
  });

  test("save button disabled when form is clean", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
  });

  test("noAuth prop shows disabled badge on auth sections", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getAllByText } = render(<ServiceSettings dark noAuth />, {
      wrapper: settingsWrapper(qc),
    });

    // Both Authentication and Password Policy get "disabled" badge
    expect(getAllByText("disabled").length).toBe(2);
  });

  test("broadcasting section shows broadcast interval", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["settings"], sampleSettings);
    qc.setQueryData(["certificates"], { certificates: [] });

    const { getByText, getByDisplayValue } = render(<ServiceSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Broadcasting"));
    expect(getByText("Broadcast Interval")).toBeTruthy();
    expect(getByDisplayValue("5s")).toBeTruthy();
  });
});
