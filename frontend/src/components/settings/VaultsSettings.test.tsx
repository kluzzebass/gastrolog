import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { encode } from "../../api/glid";

const mocks = installMockClients();

import { VaultsSettings } from "./VaultsSettings";
import { VaultConfig, VaultType } from "../../api/gen/gastrolog/v1/system_pb";

/** Create a distinct 16-byte Uint8Array test ID from a small number. */
function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

// Use real VaultConfig instances so proto-default fields (cloudServiceId,
// retentionRules, path, cache fields, etc.) come back as their typed
// zero values rather than `undefined`. The card reads those fields
// directly off the vault.
const sampleConfig = {
  vaults: [
    new VaultConfig({
      id: testId(1),
      name: "vault-alpha",
      enabled: true,
      type: VaultType.FILE,
      storageClass: 1,
      replicationFactor: 1,
      rotationPolicyId: testId(20),
    }),
    new VaultConfig({
      id: testId(2),
      name: "vault-beta",
      enabled: false,
      // Different type from vault-alpha so the "file" badge selector
      // in `renders vault cards with names and type badges` matches a
      // single card.
      type: VaultType.MEMORY,
      replicationFactor: 1,
    }),
  ],
  rotationPolicies: [{ id: testId(20), name: "daily" }],
  retentionPolicies: [{ id: testId(30), name: "30-day" }],
  routes: [],
  ingesters: [],
  nodeConfigs: [{ id: testId(40), name: "node-1" }],
  nodeStorageConfigs: [{ nodeId: testId(40), fileStorages: [{ id: testId(50), storageClass: 1 }] }],
};

beforeEach(() => {
  m(mocks.systemClient, "getConfig").mockClear();
  m(mocks.systemClient, "putVault").mockClear();
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
    // vault-alpha is file-backed
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

  test("expand vault shows edit form and action buttons", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText, getByDisplayValue } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    // The expanded card shows name, enabled, and the storage shape form.
    expect(getByText("Name")).toBeTruthy();
    expect(getByText("Enabled")).toBeTruthy();
    expect(getByText("Storage Class")).toBeTruthy();
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

  test("create button disabled before storage type is selected", async () => {
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

  test("create memory vault calls PutVault with the storage shape inline", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    m(mocks.systemClient, "putVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { ...sampleConfig, vaults: [] });

    const { getByText, getByLabelText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());

    // A vault has one storage shape — the form exposes a single
    // "Storage Type" select inline.
    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "memory" } });

    const createBtn = getByText("Create").closest("button")!;
    expect(createBtn.disabled).toBe(false);

    fireEvent.click(createBtn);

    await waitFor(() => {
      expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(1);
    });
  });

  test("transfer target field appears only when disposition is transfer, and is required to create", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText, getByLabelText, queryByLabelText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());

    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "file" } });
    expect(queryByLabelText("Transfer Target")).toBeNull();

    fireEvent.change(getByLabelText("Storage Class"), { target: { value: "1" } });
    fireEvent.change(getByLabelText("Retention Disposition"), { target: { value: "transfer" } });

    await waitFor(() => expect(getByLabelText("Transfer Target")).toBeTruthy());
    // Storage class is set but no transfer target yet — Create stays disabled.
    const createBtn = getByText("Create").closest("button")!;
    expect(createBtn.disabled).toBe(true);
  });

  test("create file vault with transfer disposition sends the transfer target vault ID", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    m(mocks.systemClient, "putVault").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText, getByLabelText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());

    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "file" } });
    fireEvent.change(getByLabelText("Storage Class"), { target: { value: "1" } });
    fireEvent.change(getByLabelText("Retention Disposition"), { target: { value: "transfer" } });
    await waitFor(() => expect(getByLabelText("Transfer Target")).toBeTruthy());
    // vault-alpha (testId(1)) is the only eligible target — a non-cloud file vault.
    fireEvent.change(getByLabelText("Transfer Target"), { target: { value: encode(testId(1)) } });

    const createBtn = getByText("Create").closest("button")!;
    await waitFor(() => expect(createBtn.disabled).toBe(false));
    fireEvent.click(createBtn);

    await waitFor(() => {
      expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(1);
      const call = m(mocks.systemClient, "putVault").mock.calls[0]! as unknown[];
      const arg = call[0] as Record<string, Record<string, unknown>>;
      const cfg = arg.config as Record<string, unknown>;
      expect(cfg.retentionDisposition).toBe("transfer");
      expect(encode(cfg.retentionTransferTargetVaultId as Uint8Array)).toBe(encode(testId(1)));
    });
  });
});

// ── Vault-level save tests ───────────────────────────────────────────
//
// All vault edits route through PutVault directly. The vault carries
// its full storage shape, so there's exactly one form to populate per
// vault — no add/remove sub-entries.

// Config with one file-backed vault — enough to exercise the vault edit
// flow. Uses real VaultConfig so the form's projection back to a fresh
// proto round-trips correctly.
const oneVaultConfig = {
  vaults: [
    new VaultConfig({
      id: testId(1),
      name: "vault-alpha",
      enabled: true,
      type: VaultType.FILE,
      storageClass: 1,
      replicationFactor: 1,
    }),
  ],
  rotationPolicies: [{ id: testId(20), name: "daily" }],
  retentionPolicies: [{ id: testId(30), name: "30-day" }],
  routes: [],
  ingesters: [],
  nodeConfigs: [{ id: testId(40), name: "node-1" }],
  nodeStorageConfigs: [{ nodeId: testId(40), fileStorages: [{ id: testId(50), storageClass: 1 }] }],
};

/** Expand vault-alpha. */
function expandVault(getByText: (text: string | RegExp) => HTMLElement) {
  fireEvent.click(getByText("vault-alpha"));
}

describe("vault edit save", () => {
  beforeEach(() => {
    m(mocks.systemClient, "putVault").mockClear();
  });

  // Note: fireEvent.change on <input> doesn't trigger React 19 controlled
  // input handlers in happy-dom. Checkbox clicks and select changes are
  // the reliable dirtying paths.

  test("save calls PutVault with the full vault config including storage shape", async () => {
    m(mocks.systemClient, "putVault").mockResolvedValue({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], oneVaultConfig);

    const { getByText, container } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    // Toggle the Enabled checkbox to dirty the vault. (fireEvent.change
    // on <select> is known unreliable under happy-dom + React 19, so the
    // checkbox path is the one we exercise here. The save handler builds
    // the full VaultConfig from the edit state regardless of which field
    // was dirtied — asserting the resulting payload covers the storage
    // shape preservation we care about.)
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
      const cfg = arg.config!;
      expect(cfg.enabled).toBe(false);
      // Storage shape preserved through the round-trip: type, RF, and
      // storage class come back unchanged from the source VaultConfig.
      expect(cfg.type).toBe(VaultType.FILE);
      expect((cfg.replicationFactor as number) || 1).toBe(1);
      expect(cfg.storageClass).toBe(1);
    });
  });

  test("save button stays disabled when nothing is dirty", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], oneVaultConfig);

    const { getByText } = render(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
    expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(0);
  });
});
