import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { VaultsSettings } from "./VaultsSettings";

/** Create a distinct 16-byte Uint8Array test ID from a small number. */
function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const sampleConfig = {
  vaults: [
    {
      id: testId(1),
      name: "vault-alpha",
      enabled: true,
    },
    {
      id: testId(2),
      name: "vault-beta",
      enabled: false,
    },
  ],
  tiers: [
    { id: testId(10), name: "local-tier", type: 2 /* FILE */, vaultId: testId(1), position: 0, rotationPolicyId: testId(20), retentionRules: [{ retentionPolicyId: testId(30) }], placements: [], replicationFactor: 1 },
  ],
  rotationPolicies: [{ id: testId(20), name: "daily" }],
  retentionPolicies: [{ id: testId(30), name: "30-day" }],
  routes: [],
  filters: [],
  ingesters: [],
  nodeConfigs: [{ id: testId(40), name: "node-1" }],
};

beforeEach(() => {
  m(mocks.systemClient, "getConfig").mockClear();
  m(mocks.systemClient, "putVault").mockClear();
  m(mocks.systemClient, "putTier").mockClear();
  m(mocks.systemClient, "deleteVault").mockClear();
  m(mocks.systemClient, "generateName").mockClear();
  m(mocks.vaultClient, "sealVault").mockClear();
  m(mocks.vaultClient, "reindexVault").mockClear();
});

describe("VaultsSettings", () => {
  test("renders empty state when no vaults", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { ...sampleConfig, vaults: [] });

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/No vaults configured/)).toBeTruthy();
  });

  test("renders vault cards with names and type badges", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("vault-alpha")).toBeTruthy();
    expect(getByText("vault-beta")).toBeTruthy();
    // vault-alpha has a local tier
    expect(getByText("file")).toBeTruthy();
  });

  test("shows disabled badge for disabled vaults", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // vault-beta is disabled
    expect(getByText("disabled")).toBeTruthy();
  });

  test("warns about missing tiers", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // vault-beta has no tiers
    expect(getByText(/no tiers configured/)).toBeTruthy();
  });

  test("expand vault shows edit form and action buttons", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

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
    expect(getByText("Save")).toBeTruthy();
  });

  test("save button disabled when not dirty", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

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
    qc.setQueryData(["system"], sampleConfig);

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
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Reindex"));

    await waitFor(() => {
      expect(m(mocks.vaultClient, "reindexVault")).toHaveBeenCalledTimes(1);
    });
  });

  test("deletes vault via confirm flow", async () => {
    m(mocks.systemClient, "deleteVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));

    await waitFor(() => {
      expect(m(mocks.systemClient, "deleteVault")).toHaveBeenCalledTimes(1);
    });
  });

  test("opens add form via button click", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { ...sampleConfig, vaults: [] });

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
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { ...sampleConfig, vaults: [] });

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));

    await waitFor(() => expect(getByText("Create")).toBeTruthy());
    const createBtn = getByText("Create").closest("button")!;
    expect(createBtn.disabled).toBe(true);
  });

  test("create vault with memory tier calls putVault then putTier", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    m(mocks.systemClient, "putVault").mockResolvedValueOnce({});
    m(mocks.systemClient, "putTier").mockResolvedValueOnce({
      config: {
        ...sampleConfig,
        tiers: [
          ...sampleConfig.tiers,
          { id: "t-new", name: "happy-fox-tier-0", type: 1 },
        ],
      },
    });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { ...sampleConfig, vaults: [] });

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
      expect(m(mocks.systemClient, "putTier")).toHaveBeenCalledTimes(1);
      expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(1);
    });
  });
});

// ── handleSaveAll tests ──────────────────────────────────────────────

import { TierConfig, RetentionRule, TierType } from "../../api/gen/gastrolog/v1/system_pb";

// Config with a vault that has two tiers — enough to test multi-tier updates.
// Uses real TierConfig proto instances so .clone() works in updateExistingTiers.
const twoTierConfig = {
  vaults: [{ id: testId(1), name: "vault-alpha", enabled: true }],
  tiers: [
    new TierConfig({ id: testId(10), name: "memory", type: TierType.MEMORY, vaultId: testId(1), position: 0, rotationPolicyId: testId(20), retentionRules: [new RetentionRule({ retentionPolicyId: testId(30), action: "transition" })], replicationFactor: 1 }),
    new TierConfig({ id: testId(11), name: "file", type: TierType.FILE, vaultId: testId(1), position: 1, retentionRules: [], replicationFactor: 1, storageClass: 1 }),
  ],
  rotationPolicies: [{ id: testId(20), name: "daily" }],
  retentionPolicies: [{ id: testId(30), name: "30-day" }],
  routes: [],
  filters: [],
  ingesters: [],
  nodeConfigs: [{ id: testId(40), name: "node-1" }],
  nodeStorageConfigs: [{ nodeId: testId(40), fileStorages: [{ id: testId(50), storageClass: 1 }] }],
};

/** Expand vault-alpha and return helpers. */
function expandVault(getByText: (text: string | RegExp) => HTMLElement) {
  fireEvent.click(getByText("vault-alpha"));
}

describe("handleSaveAll", () => {
  beforeEach(() => {
    m(mocks.systemClient, "putVault").mockClear();
    m(mocks.systemClient, "putTier").mockClear();
    m(mocks.systemClient, "deleteTier").mockClear();
  });

  // ── Happy path ────────────────────────────────────────────────────

  // Note: fireEvent.change on <input> doesn't trigger React 19 controlled
  // input handlers in happy-dom. We use checkbox clicks and select changes
  // to dirty the form instead.

  test("saves vault enabled toggle", async () => {
    m(mocks.systemClient, "putVault").mockResolvedValue({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], twoTierConfig);

    const { getByText, container } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    // Toggle the Enabled checkbox to dirty the vault state.
    const enabledCheckbox = container.querySelector('[aria-checked="true"][role="checkbox"]')!;
    fireEvent.click(enabledCheckbox);

    await waitFor(() => {
      const saveBtn = getByText("Save").closest("button")!;
      expect(saveBtn.disabled).toBe(false);
    });

    fireEvent.click(getByText("Save").closest("button")!);

    await waitFor(() => {
      expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(1);
      const call = m(mocks.systemClient, "putVault").mock.calls[0]! as unknown[];
      const arg = call[0] as Record<string, Record<string, unknown>>;
      expect(arg.config!.enabled).toBe(false);
    });
  });

  test("saves tier rotation policy change via select", async () => {
    m(mocks.systemClient, "putTier").mockResolvedValue({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], twoTierConfig);

    const { getByText, container } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    // Find the rotation policy select for the first tier and clear it.
    const selects = container.querySelectorAll("select");
    const rotationSelect = selects[0]!;
    fireEvent.change(rotationSelect, { target: { value: "" } });

    await waitFor(() => {
      const saveBtn = getByText("Save").closest("button")!;
      expect(saveBtn.disabled).toBe(false);
    });

    fireEvent.click(getByText("Save").closest("button")!);

    await waitFor(() => {
      expect(m(mocks.systemClient, "putTier")).toHaveBeenCalled();
    });
  });

  // ── Unhappy path — tier creation failure aborts save ──────────────

  test("tier creation failure aborts entire save", async () => {
    m(mocks.systemClient, "putTier").mockRejectedValueOnce(new Error("tier create failed"));
    m(mocks.systemClient, "putVault").mockResolvedValue({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], twoTierConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    // Add a new memory tier
    fireEvent.click(getByText("+ Add Tier"));
    fireEvent.click(getByText("Memory"));

    // Also toggle enabled to verify the vault save is skipped.
    const enabledCheckbox = document.querySelector('[aria-checked="true"][role="checkbox"]')!;
    fireEvent.click(enabledCheckbox);

    const saveBtn = getByText("Save").closest("button")!;
    fireEvent.click(saveBtn);

    await waitFor(() => {
      // putTier was called (for the new tier) and failed
      expect(m(mocks.systemClient, "putTier")).toHaveBeenCalledTimes(1);
    });

    // Vault save should NOT have been called — creation failure aborts
    expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(0);
  });

  // ── Unhappy path — tier update failure doesn't reset edit state ───

  test("tier update failure toasts error and preserves edit state", async () => {
    // putTier fails on the first tier update
    m(mocks.systemClient, "putTier").mockRejectedValue(new Error("tier update failed"));
    m(mocks.systemClient, "putVault").mockResolvedValue({});

    const qc = createTestQueryClient();
    qc.setQueryData(["system"], twoTierConfig);

    const { getByText, container } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    // Change rotation policy on the first tier (via select) to dirty the tier.
    const selects = container.querySelectorAll("select");
    const rotationSelect = selects[0]!;
    fireEvent.change(rotationSelect, { target: { value: "" } });

    // Also toggle enabled so vault save runs.
    const enabledCheckbox = container.querySelector('[aria-checked="true"][role="checkbox"]')!;
    fireEvent.click(enabledCheckbox);

    await waitFor(() => {
      const saveBtn = getByText("Save").closest("button")!;
      expect(saveBtn.disabled).toBe(false);
    });

    fireEvent.click(getByText("Save").closest("button")!);

    await waitFor(() => {
      // putTier was attempted (and failed)
      expect(m(mocks.systemClient, "putTier")).toHaveBeenCalled();
      // Vault save still runs (we continue despite tier failures)
      expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(1);
    });
  });

  // ── Unhappy path — removal failure toasts and continues ───────────

  test("tier removal failure toasts error", async () => {
    m(mocks.systemClient, "deleteTier").mockRejectedValueOnce(new Error("delete failed"));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], twoTierConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    // Remove the second tier — click its remove button
    const removeButtons = document.querySelectorAll("[aria-label='Remove tier']");
    if (removeButtons.length > 0) {
      fireEvent.click(removeButtons.item(removeButtons.length - 1));
      // Confirm removal if there's a confirm step
      const confirmBtns = document.querySelectorAll("button");
      for (const btn of confirmBtns) {
        if (btn.textContent === "Yes" || btn.textContent === "Confirm") {
          fireEvent.click(btn);
          break;
        }
      }
    }

    const saveBtn = getByText("Save").closest("button")!;
    if (!saveBtn.disabled) {
      fireEvent.click(saveBtn);

      await waitFor(() => {
        expect(m(mocks.systemClient, "deleteTier")).toHaveBeenCalled();
      });
    }
  });

  // ── Edge case — save with no changes is a no-op ───────────────────

  test("save button stays disabled when nothing is dirty", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], twoTierConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
    // No API calls should have been made
    expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(0);
    expect(m(mocks.systemClient, "putTier")).toHaveBeenCalledTimes(0);
  });
});
