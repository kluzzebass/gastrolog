import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { RoutesSettings } from "./RoutesSettings";

const sampleConfig = {
  routes: [
    {
      id: "r1",
      name: "default-route",
      filterId: "f1",
      destinations: [{ vaultId: "v1" }],
      distribution: "fanout",
      enabled: true,
    },
    {
      id: "r2",
      name: "backup-route",
      filterId: "",
      destinations: [{ vaultId: "v2" }],
      distribution: "failover",
      enabled: false,
    },
  ],
  filters: [{ id: "f1", name: "prod-filter" }],
  vaults: [
    { id: "v1", name: "vault-alpha" },
    { id: "v2", name: "vault-beta" },
  ],
  ingesters: [],
  nodeConfigs: [],
};

beforeEach(() => {
  m(mocks.configClient, "getConfig").mockClear();
  m(mocks.configClient, "putRoute").mockClear();
  m(mocks.configClient, "deleteRoute").mockClear();
  m(mocks.configClient, "generateName").mockClear();
});

describe("RoutesSettings", () => {
  test("renders empty state when no routes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], { routes: [], filters: [], vaults: [], ingesters: [], nodeConfigs: [] });

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/No routes configured/)).toBeTruthy();
  });

  test("renders route cards with names and distribution badges", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

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
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/prod-filter/)).toBeTruthy();
    expect(getByText(/vault-alpha/)).toBeTruthy();
  });

  test("shows disabled indicator for disabled routes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/\(disabled\)/)).toBeTruthy();
  });

  test("shows no filter label when route has no filter", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getAllByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // backup-route has no filter
    expect(getAllByText(/no filter/).length).toBeGreaterThanOrEqual(1);
  });

  test("expands route card on click and shows edit form", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

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
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("default-route"));
    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
  });

  test("deletes route via confirm flow", async () => {
    m(mocks.configClient, "deleteRoute").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("default-route"));
    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));

    await waitFor(() => {
      expect(m(mocks.configClient, "deleteRoute")).toHaveBeenCalledTimes(1);
    });
  });

  test("opens add form via Add Route button", async () => {
    m(mocks.configClient, "generateName").mockResolvedValueOnce({ name: "lucky-panda" });
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], { routes: [], filters: [], vaults: [{ id: "v1", name: "vault-alpha" }], ingesters: [], nodeConfigs: [] });

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
