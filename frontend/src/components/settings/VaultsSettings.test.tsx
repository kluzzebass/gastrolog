import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { VaultsSettings } from "./VaultsSettings";

const sampleConfig = {
  vaults: [
    {
      id: "v1",
      name: "vault-alpha",
      type: "file",
      policy: "p1",
      retentionRules: [{ retentionPolicyId: "rp1", action: "delete", destinationId: "" }],
      enabled: true,
      params: { dir: "/data/alpha" },
      nodeId: "n1",
    },
    {
      id: "v2",
      name: "vault-beta",
      type: "memory",
      policy: "",
      retentionRules: [],
      enabled: false,
      params: {},
      nodeId: "",
    },
  ],
  rotationPolicies: [{ id: "p1", name: "daily" }],
  retentionPolicies: [{ id: "rp1", name: "30-day" }],
  routes: [],
  filters: [],
  ingesters: [],
  nodeConfigs: [{ id: "n1", name: "node-1" }],
};

beforeEach(() => {
  m(mocks.configClient, "getConfig").mockClear();
  m(mocks.configClient, "putVault").mockClear();
  m(mocks.configClient, "deleteVault").mockClear();
  m(mocks.configClient, "generateName").mockClear();
  m(mocks.vaultClient, "sealVault").mockClear();
  m(mocks.vaultClient, "reindexVault").mockClear();
});

describe("VaultsSettings", () => {
  test("renders empty state when no vaults", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], { ...sampleConfig, vaults: [] });

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/No vaults configured/)).toBeTruthy();
  });

  test("renders vault cards with names and type badges", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("vault-alpha")).toBeTruthy();
    expect(getByText("vault-beta")).toBeTruthy();
    expect(getByText("file")).toBeTruthy();
    expect(getByText("memory")).toBeTruthy();
  });

  test("shows disabled badge for disabled vaults", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // vault-beta is disabled
    expect(getByText("disabled")).toBeTruthy();
  });

  test("warns about missing rotation and retention policies", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // vault-beta has no policy and no retention rules
    expect(getByText(/no rotation policy/)).toBeTruthy();
    expect(getByText(/no retention policy/)).toBeTruthy();
  });

  test("expand vault shows edit form and action buttons", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText, getByDisplayValue } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    // Form fields
    expect(getByText("Name")).toBeTruthy();
    expect(getByText("Enabled")).toBeTruthy();
    expect(getByText("Rotation Policy")).toBeTruthy();
    expect(getByDisplayValue("vault-alpha")).toBeTruthy();
    // Action buttons
    expect(getByText("Rotate")).toBeTruthy();
    expect(getByText("Reindex")).toBeTruthy();
    expect(getByText("Migrate")).toBeTruthy();
    expect(getByText("Merge Into...")).toBeTruthy();
    expect(getByText("Save")).toBeTruthy();
  });

  test("save button disabled when not dirty", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
  });

  test("rotate calls sealVault API", async () => {
    m(mocks.vaultClient, "sealVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Rotate"));

    await waitFor(() => {
      expect(m(mocks.vaultClient, "sealVault")).toHaveBeenCalledTimes(1);
    });
  });

  test("reindex calls reindexVault API", async () => {
    m(mocks.vaultClient, "reindexVault").mockResolvedValueOnce({ jobId: "j1" });
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Reindex"));

    await waitFor(() => {
      expect(m(mocks.vaultClient, "reindexVault")).toHaveBeenCalledTimes(1);
    });
  });

  test("migrate button toggles migrate form", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Migrate"));
    // Migrate form should appear
    expect(getByText("Migrate Vault")).toBeTruthy();
    expect(getByText("Destination Name")).toBeTruthy();
    expect(getByText("Cancel Migrate")).toBeTruthy();
  });

  test("merge button toggles merge form", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Merge Into..."));
    // Merge form should appear
    expect(getByText("Merge Into Another Vault")).toBeTruthy();
    expect(getByText("Destination")).toBeTruthy();
    expect(getByText("Cancel Merge")).toBeTruthy();
  });

  test("deletes vault via confirm flow", async () => {
    m(mocks.configClient, "deleteVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));

    await waitFor(() => {
      expect(m(mocks.configClient, "deleteVault")).toHaveBeenCalledTimes(1);
    });
  });

  test("opens add form via dropdown selection", async () => {
    m(mocks.configClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], { ...sampleConfig, vaults: [] });

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // Open dropdown
    fireEvent.click(getByText("Add Vault"));
    // Select type
    fireEvent.click(getByText("memory"));

    await waitFor(() => {
      expect(getByText("Create")).toBeTruthy();
      expect(getByText("Name")).toBeTruthy();
      expect(getByText("Rotation Policy")).toBeTruthy();
    });
  });

  test("create vault calls API", async () => {
    m(mocks.configClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    m(mocks.configClient, "putVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], { ...sampleConfig, vaults: [] });

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));
    fireEvent.click(getByText("memory"));

    await waitFor(() => expect(getByText("Create")).toBeTruthy());
    fireEvent.click(getByText("Create"));

    await waitFor(() => {
      expect(m(mocks.configClient, "putVault")).toHaveBeenCalledTimes(1);
    });
  });
});
