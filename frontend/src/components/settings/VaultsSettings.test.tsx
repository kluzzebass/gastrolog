import { describe, test, expect, beforeEach, mock } from "bun:test";
import { act, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { encode } from "../../api/glid";
import { HelpProvider } from "../../hooks/useHelp";

const mocks = installMockClients();

import { VaultsSettings } from "./VaultsSettings";
import { TRANSFER_HELP_REF, transferSourceIneligibility, transferTargetOptions } from "./transferEligibility";
import { GetSystemResponse, RetentionRule, VaultConfig, VaultPlacement, VaultType } from "../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../testing/renderWritable";

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

    const { getByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/No vaults configured/)).toBeTruthy();
  });

  test("renders vault cards with names and type badges", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("vault-alpha")).toBeTruthy();
    expect(getByText("vault-beta")).toBeTruthy();
    // vault-alpha is file-backed
    expect(getByText("file")).toBeTruthy();
  });

  test("a cloud-backed vault's badge carries the canonical term and the service name", () => {
    const qc = createTestQueryClient();
    const cloudId = testId(60);
    qc.setQueryData(["system"], {
      ...sampleConfig,
      cloudServices: [{ id: cloudId, name: "minio" }],
      vaults: [
        new VaultConfig({
          id: testId(3),
          name: "vault-cloud",
          enabled: true,
          type: VaultType.FILE,
          storageClass: 1,
          replicationFactor: 1,
          cloudServiceId: cloudId,
        }),
      ],
    });

    const { getByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    const badge = getByText("cloud-backed: minio");
    expect(badge.getAttribute("title")).toContain("sealed chunks upload to cloud service minio");
  });

  test("shows disabled badge for disabled vaults", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // vault-beta is disabled
    expect(getByText("disabled")).toBeTruthy();
  });

  test("expand vault shows edit form and action buttons", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText, getByDisplayValue } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
  });

  test("rotate calls sealVault API and reports the chunks sealed", async () => {
    m(mocks.vaultClient, "sealVault").mockResolvedValueOnce({ sealedCount: 2 });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText, findByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Rotate"));

    await waitFor(() => {
      expect(m(mocks.vaultClient, "sealVault")).toHaveBeenCalledTimes(1);
    });
    expect(await findByText("Sealed 2 open chunk(s)")).toBeTruthy();
  });

  test("rotate with nothing open says nothing was sealed", async () => {
    m(mocks.vaultClient, "sealVault").mockResolvedValueOnce({ sealedCount: 0 });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText, findByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-alpha"));
    fireEvent.click(getByText("Rotate"));

    expect(await findByText("Nothing to rotate: no open chunk holds records")).toBeTruthy();
  });

  test("reindex calls reindexVault API", async () => {
    m(mocks.vaultClient, "reindexVault").mockResolvedValueOnce({ jobId: "j1" });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText, getByLabelText } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText, getByLabelText, queryByLabelText } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText, getByLabelText } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText, container } = renderWritable(<VaultsSettings dark />, {
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

    const { getByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);

    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
    expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(0);
  });
});

// ── Transfer eligibility ─────────────────────────────────────────────
//
// Each reason the picker shows corresponds to a PutVault rejection:
// self-transfer, a non-file target, a cloud-backed target, a target whose
// transfer chain cycles, and a source that is not a non-cloud file vault.

function fileVault(n: number, name: string, extra: Partial<VaultConfig> = {}): VaultConfig {
  return new VaultConfig({
    id: testId(n),
    name,
    enabled: true,
    type: VaultType.FILE,
    storageClass: 1,
    replicationFactor: 1,
    ...extra,
  });
}

function transferringTo(target: number): Partial<VaultConfig> {
  return { retentionDisposition: "transfer", retentionTransferTargetVaultId: testId(target) };
}

function optionStates(select: HTMLElement): { label: string; disabled: boolean }[] {
  return [...select.querySelectorAll("option")].map((o) => ({ label: o.textContent, disabled: o.disabled }));
}

function reasonsByLabel(options: { label: string; ineligibleReason?: string }[]): Record<string, string | undefined> {
  return Object.fromEntries(options.map((o) => [o.label, o.ineligibleReason]));
}

describe("transferTargetOptions", () => {
  test("names the PutVault rejection for every ineligible vault", () => {
    const vaults = [
      fileVault(1, "source"),
      fileVault(2, "plain"),
      fileVault(3, "cloudy", { cloudServiceId: testId(60) }),
      new VaultConfig({ id: testId(4), name: "ram", type: VaultType.MEMORY }),
      new VaultConfig({ id: testId(5), name: "sink", type: VaultType.JSONL }),
    ];
    expect(reasonsByLabel(transferTargetOptions(vaults, encode(testId(1))))).toEqual({
      source: "this vault",
      plain: undefined,
      cloudy: "cloud-backed",
      ram: "memory vault",
      sink: "JSONL sink",
    });
  });

  test("a target whose transfer chain leads back to the source would create a cycle", () => {
    // c → source, b → c, d → b: each of them closes a loop through source.
    const vaults = [
      fileVault(1, "source"),
      fileVault(2, "b", transferringTo(3)),
      fileVault(3, "c", transferringTo(1)),
      fileVault(4, "d", transferringTo(2)),
      fileVault(5, "e"),
    ];
    expect(reasonsByLabel(transferTargetOptions(vaults, encode(testId(1))))).toEqual({
      source: "this vault",
      b: "would create a transfer cycle",
      c: "would create a transfer cycle",
      d: "would create a transfer cycle",
      e: undefined,
    });
  });

  test("a target ID on a vault whose disposition is not transfer is no edge", () => {
    const vaults = [
      fileVault(1, "source"),
      fileVault(2, "b", { retentionDisposition: "delete", retentionTransferTargetVaultId: testId(1) }),
      fileVault(3, "c", transferringTo(2)),
    ];
    expect(reasonsByLabel(transferTargetOptions(vaults, encode(testId(1))))).toEqual({
      source: "this vault",
      b: undefined,
      c: undefined,
    });
  });

  test("an existing cycle elsewhere in the graph makes its members ineligible for a new vault", () => {
    const vaults = [fileVault(2, "b", transferringTo(3)), fileVault(3, "c", transferringTo(2)), fileVault(4, "d")];
    expect(reasonsByLabel(transferTargetOptions(vaults))).toEqual({
      b: "would create a transfer cycle",
      c: "would create a transfer cycle",
      d: undefined,
    });
  });

  test("disabled vaults stay eligible — PutVault does not check enabled", () => {
    const vaults = [fileVault(1, "source"), fileVault(2, "off", { enabled: false })];
    expect(reasonsByLabel(transferTargetOptions(vaults, encode(testId(1)))).off).toBeUndefined();
  });

  test("lists eligible vaults first, each group sorted by name", () => {
    const vaults = [
      fileVault(1, "zulu"),
      fileVault(2, "alpha", { cloudServiceId: testId(60) }),
      fileVault(3, "mike"),
      fileVault(4, "bravo"),
    ];
    expect(transferTargetOptions(vaults, encode(testId(1))).map((o) => o.label)).toEqual([
      "bravo",
      "mike",
      "alpha",
      "zulu",
    ]);
  });
});

// ── Placement block, RF cap, storage class labels ────────────────────
//
// Two nodes, each with one class-1 file storage: the RF cap for class 1
// is 2, and a vault placed on both storages is at RF=2.

const twoNodeConfig = {
  ...oneVaultConfig,
  nodeConfigs: [
    { id: testId(40), name: "node-1" },
    { id: testId(41), name: "node-2" },
  ],
  nodeStorageConfigs: [
    { nodeId: testId(40), fileStorages: [{ id: testId(50), storageClass: 1 }] },
    { nodeId: testId(41), fileStorages: [{ id: testId(51), storageClass: 1 }] },
  ],
};

function placedOnBoth(rf: number): VaultConfig {
  return fileVault(1, "vault-alpha", {
    replicationFactor: rf,
    placements: [
      new VaultPlacement({ storageId: testId(50), leader: true }),
      new VaultPlacement({ storageId: testId(51), leader: false }),
    ],
  });
}

function renderVaults(config: unknown) {
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], config);
  return renderWritable(<VaultsSettings dark />, { wrapper: settingsWrapper(qc) });
}

describe("placement block", () => {
  test("a placed vault names its leader, followers, RF and the cap with its cause", () => {
    const { getByText, queryByText } = renderVaults({ ...twoNodeConfig, vaults: [placedOnBoth(2)] });
    expandVault(getByText);

    expect(getByText("Placement")).toBeTruthy();
    expect(getByText("leader: node-1 · followers: node-2")).toBeTruthy();
    expect(getByText("RF 2 · cap 2 (2 file storages with class 1)")).toBeTruthy();
    expect(queryByText(/short by/)).toBeNull();
    expect(queryByText(/placements —/)).toBeNull();
  });

  test("a vault with no storage of its class is unplaced, with the cause and the action", () => {
    const vault = fileVault(1, "vault-alpha", { storageClass: 3 });
    const { getByText } = renderVaults({ ...twoNodeConfig, vaults: [vault] });

    // The header counts placements and names the action while collapsed.
    expect(getByText("0 of 1 placement — add 1 file storage with class 3")).toBeTruthy();
    expandVault(getByText);
    expect(getByText("unplaced — no file storage has class 3")).toBeTruthy();
    expect(getByText("RF 1 · cap 1 (no file storage has class 3)")).toBeTruthy();
  });

  test("a vault with eligible storages but no leader is unplaced pending placement", () => {
    const { getByText } = renderVaults({ ...twoNodeConfig, vaults: [fileVault(1, "vault-alpha")] });
    expect(getByText("0 of 1 placement — placement pending")).toBeTruthy();
    expandVault(getByText);
    expect(getByText("unplaced — no leader assigned yet")).toBeTruthy();
  });

  test("an RF shortfall counts placements and says what raises the count", () => {
    const { getByText } = renderVaults({ ...twoNodeConfig, vaults: [placedOnBoth(3)] });
    expect(getByText("2 of 3 placements — add 1 file storage with class 1")).toBeTruthy();
    expandVault(getByText);
    expect(getByText("short by 1 placement — add 1 file storage with class 1")).toBeTruthy();
  });

  test("with enough storages for the RF the shortfall is pending placement, not a missing storage", () => {
    const config = {
      ...twoNodeConfig,
      vaults: [placedOnBoth(3)],
      nodeStorageConfigs: [
        ...twoNodeConfig.nodeStorageConfigs,
        { nodeId: testId(41), fileStorages: [{ id: testId(52), storageClass: 1 }] },
      ],
    };
    const { getByText } = renderVaults(config);
    expect(getByText("2 of 3 placements — placement pending")).toBeTruthy();
  });

  test("the placement block links to its help section", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { ...twoNodeConfig, vaults: [placedOnBoth(2)] });
    const openHelp = mock((_helpRef?: string) => {});
    const { getByText } = renderWritable(
      <HelpProvider onOpen={openHelp}>
        <VaultsSettings dark />
      </HelpProvider>,
      { wrapper: settingsWrapper(qc) },
    );
    expandVault(getByText);
    fireEvent.click(getByText("Placement").parentElement!.querySelector('button[aria-label="Help"]')!);
    expect(openHelp).toHaveBeenCalledWith("vaults-config#placement");
  });
});

describe("replication factor cap", () => {
  test("the field states the cap and the storages it derives from", () => {
    const { getByText } = renderVaults({ ...twoNodeConfig, vaults: [placedOnBoth(2)] });
    expandVault(getByText);
    expect(getByText(/Cap 2: 2 file storages with class 1\./)).toBeTruthy();
  });

  test("dropping a storage from the class lowers the stated cap", () => {
    const config = { ...twoNodeConfig, vaults: [placedOnBoth(2)], nodeStorageConfigs: twoNodeConfig.nodeStorageConfigs.slice(0, 1) };
    const { getByText, queryByText } = renderVaults(config);
    expandVault(getByText);
    expect(queryByText(/Cap 2:/)).toBeNull();
    expect(getByText(/Cap 1: 1 file storage with class 1\./)).toBeTruthy();
  });

  test("a memory vault's cap is the node count", () => {
    const vault = new VaultConfig({ id: testId(1), name: "vault-alpha", type: VaultType.MEMORY, replicationFactor: 1 });
    const { getByText } = renderVaults({ ...twoNodeConfig, vaults: [vault] });
    expandVault(getByText);
    expect(getByText(/Cap 2: 2 nodes in the cluster\./)).toBeTruthy();
  });
});

describe("storage class options", () => {
  test("each class names its file storage count and nodes", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const { getByText, getByLabelText } = renderVaults({ ...twoNodeConfig, vaults: [] });
    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());
    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "file" } });

    expect(optionStates(getByLabelText("Storage Class")).map((o) => o.label)).toEqual([
      "Select storage class...",
      "Class 1 — 2 file storages on node-1, node-2",
    ]);
  });
});

// ── Retention policies: multi-attach ─────────────────────────────────

const twoPoliciesConfig = {
  ...oneVaultConfig,
  retentionPolicies: [
    { id: testId(30), name: "30-day" },
    { id: testId(31), name: "90-day" },
  ],
};

function checkboxFor(getByText: (text: string) => HTMLElement, label: string): HTMLElement {
  return getByText(label).closest('[role="checkbox"]')!;
}

async function savedRetentionPolicyIds(): Promise<string[]> {
  let ids: string[] = [];
  await waitFor(() => {
    expect(m(mocks.systemClient, "putVault")).toHaveBeenCalledTimes(1);
    const call = m(mocks.systemClient, "putVault").mock.calls[0]! as unknown[];
    const arg = call[0] as { config: { retentionRules: { retentionPolicyId: Uint8Array }[] } };
    ids = arg.config.retentionRules.map((r) => encode(r.retentionPolicyId));
  });
  return ids;
}

describe("retention policies", () => {
  test("every attached policy shows checked, and detaching one saves the rest", async () => {
    m(mocks.systemClient, "putVault").mockClear();
    m(mocks.systemClient, "putVault").mockResolvedValue({});
    const vault = fileVault(1, "vault-alpha", {
      retentionRules: [
        new RetentionRule({ retentionPolicyId: testId(30) }),
        new RetentionRule({ retentionPolicyId: testId(31) }),
      ],
    });
    const { getByText } = renderVaults({ ...twoPoliciesConfig, vaults: [vault] });
    expandVault(getByText);

    expect(getByText("2 policies attached. Every attached policy is evaluated on each sweep; the tightest bound wins.")).toBeTruthy();
    expect(checkboxFor(getByText, "30-day").getAttribute("aria-checked")).toBe("true");
    expect(checkboxFor(getByText, "90-day").getAttribute("aria-checked")).toBe("true");

    fireEvent.click(checkboxFor(getByText, "30-day"));
    expect(getByText("1 policy attached. Every attached policy is evaluated on each sweep; the tightest bound wins.")).toBeTruthy();
    fireEvent.click(getByText("Save").closest("button")!);

    expect(await savedRetentionPolicyIds()).toEqual([encode(testId(31))]);
  });

  test("attaching policies saves one rule per policy in attachment order", async () => {
    m(mocks.systemClient, "putVault").mockClear();
    m(mocks.systemClient, "putVault").mockResolvedValue({});
    const { getByText } = renderVaults(twoPoliciesConfig);
    expandVault(getByText);

    expect(getByText("None attached: sealed chunks stay until the default size floor drains them.")).toBeTruthy();
    fireEvent.click(checkboxFor(getByText, "90-day"));
    fireEvent.click(checkboxFor(getByText, "30-day"));
    fireEvent.click(getByText("Save").closest("button")!);

    expect(await savedRetentionPolicyIds()).toEqual([encode(testId(31)), encode(testId(30))]);
  });

  test("creating a vault attaches every checked policy", async () => {
    m(mocks.systemClient, "putVault").mockClear();
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    m(mocks.systemClient, "putVault").mockResolvedValueOnce({});
    const { getByText, getByLabelText } = renderVaults({ ...twoPoliciesConfig, vaults: [] });
    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());
    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "memory" } });
    fireEvent.click(checkboxFor(getByText, "30-day"));
    fireEvent.click(checkboxFor(getByText, "90-day"));
    fireEvent.click(getByText("Create").closest("button")!);

    expect(await savedRetentionPolicyIds()).toEqual([encode(testId(30)), encode(testId(31))]);
  });
});

describe("vault actions", () => {
  test("each action states its consequence beside the button", () => {
    const { getByText } = renderVaults(oneVaultConfig);
    expandVault(getByText);
    expect(getByText("Actions")).toBeTruthy();
    expect(getByText("Seals every active chunk holding records; the next record opens a new one.")).toBeTruthy();
    expect(getByText("Rebuilds every sealed chunk's indexes as a tracked job; records are untouched.")).toBeTruthy();
    expect(getByText("Resets the retry backoff on unreadable chunks so the next retention sweep retries them all.")).toBeTruthy();
  });
});

describe("transferSourceIneligibility", () => {
  test("only a non-cloud file vault may transfer", () => {
    expect(transferSourceIneligibility({ type: "file", cloudServiceId: "" })).toBeUndefined();
    expect(transferSourceIneligibility({ type: "file", cloudServiceId: "x" })).toBe("this vault is cloud-backed");
    expect(transferSourceIneligibility({ type: "memory", cloudServiceId: "" })).toBe("this is a memory vault");
    expect(transferSourceIneligibility({ type: "jsonl", cloudServiceId: "" })).toBe("this is a JSONL sink");
  });
});

describe("transfer target picker", () => {
  // The two-vault cluster where nothing qualifies: the source is excluded
  // as itself, the only other vault as cloud-backed.
  const noEligibleConfig = {
    ...oneVaultConfig,
    vaults: [fileVault(1, "vault-alpha"), fileVault(2, "vault-cloud", { cloudServiceId: testId(60) })],
    cloudServices: [{ id: testId(60), name: "s3" }],
  };

  test("an empty picker names the rule that emptied it and links to it", async () => {
    const { getByText, getByLabelText, openHelp } = renderWithHelp(noEligibleConfig);
    expandVault(getByText);
    fireEvent.change(getByLabelText("Retention Disposition"), { target: { value: "transfer" } });

    await waitFor(() => expect(getByLabelText("Transfer Target")).toBeTruthy());
    expect(fieldNote(getByLabelText("Transfer Target"))?.textContent).toContain(
      "No eligible targets — transfer requires a different, non-cloud file vault.",
    );

    fireEvent.click(getByText("Eligibility rules"));
    expect(openHelp).toHaveBeenCalledWith(TRANSFER_HELP_REF);
  });

  test("the replication factor field opens the file storage placement section", () => {
    const { getByText, openHelp } = renderWithHelp(oneVaultConfig);
    expandVault(getByText);

    const label = getByText("Replication Factor");
    const button = label.parentElement!.querySelector('button[aria-label="Help"]');
    expect(button).not.toBeNull();
    fireEvent.click(button!);

    expect(openHelp).toHaveBeenCalledWith("storage-config#how-vaults-use-file-storages");
  });

  test("ineligible vaults are listed disabled with their reason", async () => {
    const { getByText, getByLabelText } = renderWithHelp(noEligibleConfig);
    expandVault(getByText);
    fireEvent.change(getByLabelText("Retention Disposition"), { target: { value: "transfer" } });

    await waitFor(() => expect(getByLabelText("Transfer Target")).toBeTruthy());
    expect(optionStates(getByLabelText("Transfer Target"))).toEqual([
      { label: "Select target vault...", disabled: false },
      { label: "vault-alpha — this vault", disabled: true },
      { label: "vault-cloud — cloud-backed", disabled: true },
    ]);
  });

  test("no empty-state note while an eligible target exists", async () => {
    const config = { ...noEligibleConfig, vaults: [...noEligibleConfig.vaults, fileVault(3, "vault-archive")] };
    const { getByText, getByLabelText } = renderWithHelp(config);
    expandVault(getByText);
    fireEvent.change(getByLabelText("Retention Disposition"), { target: { value: "transfer" } });

    await waitFor(() => expect(getByLabelText("Transfer Target")).toBeTruthy());
    expect(fieldNote(getByLabelText("Transfer Target"))).toBeNull();
    expect(optionStates(getByLabelText("Transfer Target"))[1]).toEqual({ label: "vault-archive", disabled: false });
  });

  test("a memory vault cannot pick transfer, and says why", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const { getByText, getByLabelText } = renderWithHelp(noEligibleConfig);
    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());

    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "memory" } });
    expect(optionStates(getByLabelText("Retention Disposition")).at(-1)).toEqual({
      label: "Transfer records to another vault unchanged — this is a memory vault",
      disabled: true,
    });
  });

  test("binding a cloud service after choosing transfer blocks Create and names the cause", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const config = { ...noEligibleConfig, vaults: [...noEligibleConfig.vaults, fileVault(3, "vault-archive")] };
    const { getByText, getByLabelText } = renderWithHelp(config);
    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());

    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "file" } });
    fireEvent.change(getByLabelText("Storage Class"), { target: { value: "1" } });
    fireEvent.change(getByLabelText("Retention Disposition"), { target: { value: "transfer" } });
    await waitFor(() => expect(getByLabelText("Transfer Target")).toBeTruthy());
    fireEvent.change(getByLabelText("Transfer Target"), { target: { value: encode(testId(3)) } });
    const createBtn = getByText("Create").closest("button")!;
    await waitFor(() => expect(createBtn.disabled).toBe(false));

    fireEvent.change(getByLabelText("Cloud Storage"), { target: { value: encode(testId(60)) } });
    await waitFor(() => expect(createBtn.disabled).toBe(true));
    expect(fieldNote(getByLabelText("Retention Disposition"))?.textContent).toContain(
      "Transfer records to another vault unchanged is not eligible — this vault is cloud-backed.",
    );
  });
});

// Real messages: the system query compares writes with the proto's own
// equals, which needs every repeated field present.
const placedVault = new VaultConfig({
  id: testId(1),
  name: "vault-alpha",
  enabled: true,
  type: VaultType.FILE,
  storageClass: 1,
  replicationFactor: 1,
  placements: [{ storageId: testId(50), leader: true }],
});
const gatedInit = {
  vaults: [placedVault],
  nodeConfigs: [{ id: testId(40), name: "node-1" }, { id: testId(41), name: "node-2" }],
  nodeStorageConfigs: [{ nodeId: testId(40), fileStorages: [{ id: testId(50), storageClass: 1 }] }],
};
const gatedConfig = new GetSystemResponse(gatedInit);

describe("VaultsSettings create gates", () => {
  test("a name conflict disables Create and says which name is taken, until the clash goes", async () => {
    // With no generated name the form falls back to "vault", which a vault
    // of that name takes — no async placeholder in the path.
    m(mocks.systemClient, "generateName").mockRejectedValueOnce(new Error("unavailable"));
    const taken = new VaultConfig({ id: testId(3), name: "vault", enabled: true, type: VaultType.MEMORY, replicationFactor: 1 });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], new GetSystemResponse({ ...gatedInit, vaults: [placedVault, taken] }));
    const { getByText, queryByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));
    expect(getByText('A vault named "vault" already exists')).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    // The query cache notifies its observers on a macrotask; flush it.
    await act(async () => {
      qc.setQueryData(["system"], gatedConfig);
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    expect(queryByText('A vault named "vault" already exists')).toBeNull();
  });

  test("a missing storage class disables Create with the reason, until one is picked", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], gatedConfig);
    const { getByText, findByText, getByLabelText, queryByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Vault"));
    expect(await findByText("Pick a storage class")).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    fireEvent.change(getByLabelText("Storage Class"), { target: { value: "1" } });
    await waitFor(() => {
      expect(queryByText("Pick a storage class")).toBeNull();
      expect(getByText("Create").closest("button")!.disabled).toBe(false);
    });
  });

});

describe("VaultsSettings delete gates and consequences", () => {
  test("a vault a route sends to cannot be deleted, and the reason names the route", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], new GetSystemResponse({
      ...gatedInit,
      routes: [{ id: testId(70), name: "to-alpha", destinations: [{ vaultId: testId(1) }] }],
    }));
    const { getByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);
    expect(getByText("Delete").closest("button")!.disabled).toBe(true);
    expect(getByText("Destination of route to-alpha — remove it there first")).toBeTruthy();
  });

  test("with no route reference Delete is enabled", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], gatedConfig);
    const { getByText, queryByText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);
    expect(getByText("Delete").closest("button")!.disabled).toBe(false);
    expect(queryByText(/remove it there first/)).toBeNull();
  });

  test("the confirm step names the placements and says what happens to the data files", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], gatedConfig);
    const { getByText, getByLabelText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expandVault(getByText);
    fireEvent.click(getByText("Delete"));

    expect(getByText(/Removes "vault-alpha" and its 1 placement\(s\) on node-1/)).toBeTruthy();
    expect(getByText(/chunk files stay on disk/)).toBeTruthy();

    fireEvent.click(getByLabelText("Delete data files"));
    expect(getByText(/chunk files are erased from every placement node/)).toBeTruthy();
    expect(getByText("Yes")).toBeTruthy();
  });

  test("a memory vault's confirm step says its records go with it", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], new GetSystemResponse({
      ...gatedInit,
      vaults: [new VaultConfig({ id: testId(2), name: "vault-mem", enabled: true, type: VaultType.MEMORY, replicationFactor: 1 })],
    }));
    const { getByText, queryByLabelText } = renderWritable(<VaultsSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("vault-mem"));
    fireEvent.click(getByText("Delete"));

    expect(getByText(/Removes "vault-mem", which has no placements/)).toBeTruthy();
    expect(getByText(/live only in memory/)).toBeTruthy();
    expect(queryByLabelText("Delete data files")).toBeNull();
  });
});

/** The note rendered inside the FormField that owns this input, if any. */
function fieldNote(input: HTMLElement): Element | null {
  return input.closest(".flex-col")!.querySelector('[role="note"]');
}

/** Render the vaults tab under a help provider whose calls are recorded. */
function renderWithHelp(config: unknown) {
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], config);
  const openHelp = mock((_helpRef?: string) => {});
  const result = renderWritable(
    <HelpProvider onOpen={openHelp}>
      <VaultsSettings dark />
    </HelpProvider>,
    { wrapper: settingsWrapper(qc) },
  );
  return { ...result, openHelp };
}

// ── Pickers that used to go empty silently, and fields that used to vanish ──

describe("vault form eligibility and gating", () => {
  const bare = {
    ...oneVaultConfig,
    rotationPolicies: [],
    retentionPolicies: [],
    cloudServices: [],
    nodeStorageConfigs: [],
  };

  test("with no file storage the Storage Class picker says so and links to the rule — no spinner fallback", () => {
    const { getByText, getByLabelText, openHelp } = renderWithHelp(bare);
    expandVault(getByText);
    expect(fieldNote(getByLabelText("Storage Class"))?.textContent).toContain("No file storage configured");
    expect(getByLabelText("Storage Class").tagName).toBe("SELECT");
    const link = fieldNote(getByLabelText("Storage Class"))!.querySelector("button")!;
    fireEvent.click(link);
    expect(openHelp).toHaveBeenCalledWith("storage-config#file-storage");
  });

  test("with no cloud storage the new-vault Cloud Storage picker says so and stays local-only", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const { getByText, getByLabelText, openHelp } = renderWithHelp(bare);
    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());
    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "file" } });
    expect((getByLabelText("Cloud Storage") as HTMLSelectElement).disabled).toBe(false);
    const note = fieldNote(getByLabelText("Cloud Storage"))!;
    expect(note.textContent).toContain("No cloud storage configured");
    fireEvent.click(note.querySelector("button")!);
    expect(openHelp).toHaveBeenCalledWith("storage-config#cloud-storage");
  });

  test("the policy pickers stay on screen with no policy configured, each naming the cause", () => {
    const { getByText, getByLabelText, openHelp } = renderWithHelp(bare);
    expandVault(getByText);
    expect(fieldNote(getByLabelText("Rotation Policy"))?.textContent).toContain("No rotation policies configured");
    expect(fieldNote(getByText("Retention Policies"))?.textContent).toContain("No retention policies configured");
    fireEvent.click(fieldNote(getByText("Retention Policies"))!.querySelector("button")!);
    expect(openHelp).toHaveBeenCalledWith("vaults-config#rotation-and-retention-policies");
  });

  test("with a policy configured the pickers carry no note", () => {
    const { getByText, getByLabelText } = renderWithHelp(oneVaultConfig);
    expandVault(getByText);
    expect(fieldNote(getByLabelText("Rotation Policy"))).toBeNull();
    expect(fieldNote(getByText("Retention Policies"))).toBeNull();
    expect([...getByLabelText("Rotation Policy").querySelectorAll("option")].map((o) => o.textContent)).toEqual(["None", "daily"]);
  });

  test("the cache fields are on screen for a local-only vault, disabled with the reason", () => {
    const { getByText, getByLabelText } = renderWithHelp(oneVaultConfig);
    expandVault(getByText);
    for (const label of ["Cache Eviction", "Cache Budget", "Cache TTL"]) {
      const input = getByLabelText(label) as HTMLInputElement;
      expect(input.disabled).toBe(true);
      expect(fieldNote(input)?.textContent).toBe("Only a cloud-backed vault has a warm cache.");
    }
  });

  test("on a cloud-backed vault the cache fields open, and Cache TTL stays gated on TTL eviction", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "happy-fox" });
    const config = { ...oneVaultConfig, cloudServices: [{ id: testId(60), name: "s3" }] };
    const { getByText, getByLabelText } = renderWithHelp(config);
    fireEvent.click(getByText("Add Vault"));
    await waitFor(() => expect(getByText("Create")).toBeTruthy());
    fireEvent.change(getByLabelText("Storage Type"), { target: { value: "file" } });
    fireEvent.change(getByLabelText("Cloud Storage"), { target: { value: encode(testId(60)) } });

    expect((getByLabelText("Cache Budget") as HTMLInputElement).disabled).toBe(false);
    expect(fieldNote(getByLabelText("Cache Budget"))).toBeNull();
    expect((getByLabelText("Cache TTL") as HTMLInputElement).disabled).toBe(true);
    expect(fieldNote(getByLabelText("Cache TTL"))?.textContent).toBe("Applies to TTL eviction only.");

    fireEvent.change(getByLabelText("Cache Eviction"), { target: { value: "ttl" } });
    expect((getByLabelText("Cache TTL") as HTMLInputElement).disabled).toBe(false);
    expect(fieldNote(getByLabelText("Cache TTL"))).toBeNull();
  });

  test("an existing vault's cloud binding is disabled with the reason it is fixed, and only that", () => {
    const { getByText, getByLabelText } = renderWithHelp(bare);
    expandVault(getByText);
    expect((getByLabelText("Cloud Storage") as HTMLSelectElement).disabled).toBe(true);
    const notes = [...getByLabelText("Cloud Storage").closest(".flex-col")!.querySelectorAll('[role="note"]')];
    expect(notes.map((n) => n.textContent)).toEqual([
      "Fixed at vault creation — to change it, create a new vault and migrate data via retention routing.",
    ]);
  });
});
