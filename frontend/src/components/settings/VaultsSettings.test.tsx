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
      tierIds: ["t1"],
      enabled: true,
    },
    {
      id: "v2",
      name: "vault-beta",
      tierIds: [],
      enabled: false,
    },
  ],
  tiers: [
    { id: "t1", name: "local-tier", type: 2 /* LOCAL */, rotationPolicyId: "p1", retentionRules: [{ retentionPolicyId: "rp1" }] },
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
  m(mocks.configClient, "putTier").mockClear();
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
    // vault-alpha has a local tier
    expect(getByText("local")).toBeTruthy();
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

  test("warns about missing tiers", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // vault-beta has no tiers
    expect(getByText(/no tiers configured/)).toBeTruthy();
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
    expect(getByText("Tiers")).toBeTruthy();
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

  test("opens add form via button click", async () => {
    m(mocks.configClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], { ...sampleConfig, vaults: [] });

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));

    await waitFor(() => {
      expect(getByText("Create")).toBeTruthy();
      expect(getByText("Name")).toBeTruthy();
    });
  });

  test("create button disabled with no tiers", async () => {
    m(mocks.configClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], { ...sampleConfig, vaults: [] });

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));

    await waitFor(() => expect(getByText("Create")).toBeTruthy());
    const createBtn = getByText("Create").closest("button")!;
    expect(createBtn.disabled).toBe(true);
  });

  test("create vault with memory tier calls putTier then putVault", async () => {
    m(mocks.configClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    m(mocks.configClient, "putTier").mockResolvedValueOnce({
      config: {
        ...sampleConfig,
        tiers: [
          ...sampleConfig.tiers,
          { id: "t-new", name: "happy-fox-tier-0", type: 1 },
        ],
      },
    });
    m(mocks.configClient, "putVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["config"], { ...sampleConfig, vaults: [] });

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());

    // Add a memory tier via the dropdown
    fireEvent.click(getByText("+ Add Tier"));
    fireEvent.click(getByText("Memory"));

    // Create button should now be enabled
    const createBtn = getByText("Create").closest("button")!;
    expect(createBtn.disabled).toBe(false);

    fireEvent.click(createBtn);

    await waitFor(() => {
      expect(m(mocks.configClient, "putTier")).toHaveBeenCalledTimes(1);
      expect(m(mocks.configClient, "putVault")).toHaveBeenCalledTimes(1);
    });
  });
});
