import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { RoutesSettings } from "./RoutesSettings";

/** Create a distinct 16-byte Uint8Array test ID from a small number. */
function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const ZERO_ID = new Uint8Array(16);

const sampleConfig = {
  routes: [
    {
      id: testId(1),
      name: "default-route",
      filterId: testId(10),
      destinations: [{ vaultId: testId(20) }],
      distribution: "fanout",
      enabled: true,
    },
    {
      id: testId(2),
      name: "backup-route",
      filterId: ZERO_ID,
      destinations: [{ vaultId: testId(21) }],
      distribution: "failover",
      enabled: false,
    },
  ],
  filters: [{ id: testId(10), name: "prod-filter" }],
  vaults: [
    { id: testId(20), name: "vault-alpha" },
    { id: testId(21), name: "vault-beta" },
  ],
  ingesters: [],
  nodeConfigs: [],
};

beforeEach(() => {
  m(mocks.systemClient, "getConfig").mockClear();
  m(mocks.systemClient, "putRoute").mockClear();
  m(mocks.systemClient, "deleteRoute").mockClear();
  m(mocks.systemClient, "generateName").mockClear();
});

describe("RoutesSettings", () => {
  test("renders empty state when no routes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { routes: [], filters: [], vaults: [], ingesters: [], nodeConfigs: [] });

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/No routes configured/)).toBeTruthy();
  });

  test("renders route cards with names and distribution badges", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("default-route")).toBeTruthy();
    expect(getByText("backup-route")).toBeTruthy();
    expect(getByText("fanout")).toBeTruthy();
    expect(getByText("failover")).toBeTruthy();
  });

  test("shows filter and destination names in route status", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/prod-filter/)).toBeTruthy();
    expect(getByText(/vault-alpha/)).toBeTruthy();
  });

  test("shows disabled indicator for disabled routes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/\(disabled\)/)).toBeTruthy();
  });

  test("shows no filter label when route has no filter", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getAllByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // backup-route has no filter
    expect(getAllByText(/no filter/).length).toBeGreaterThanOrEqual(1);
  });

  test("expands route card on click and shows edit form", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("default-route"));
    // Form fields should appear
    expect(getByText("Name")).toBeTruthy();
    expect(getByText("Filter")).toBeTruthy();
    expect(getByText("Distribution")).toBeTruthy();
    expect(getByText("Destinations")).toBeTruthy();
    expect(getByText("Enabled")).toBeTruthy();
  });

  test("save button disabled when not dirty", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("default-route"));
    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
  });

  test("deletes route via confirm flow", async () => {
    m(mocks.systemClient, "deleteRoute").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("default-route"));
    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));

    await waitFor(() => {
      expect(m(mocks.systemClient, "deleteRoute")).toHaveBeenCalledTimes(1);
    });
  });

  test("opens add form via Add Route button", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "lucky-panda" });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { routes: [], filters: [], vaults: [{ id: testId(20), name: "vault-alpha" }], ingesters: [], nodeConfigs: [] });

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Route"));
    // Add form should show Create button and form fields
    await waitFor(() => {
      expect(getByText("Create")).toBeTruthy();
      expect(getByText("Name")).toBeTruthy();
      expect(getByText("Distribution")).toBeTruthy();
    });
  });
});
