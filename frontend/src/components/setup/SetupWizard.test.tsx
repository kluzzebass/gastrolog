import { describe, test, expect, beforeEach, mock } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

mock.module("@tanstack/react-router", () => ({
  useNavigate: () => mock(() => {}),
}));

import { SetupWizard } from "./SetupWizard";
import { GetSystemResponse, VaultType } from "../../api/gen/gastrolog/v1/system_pb";
import { FileStorage, NodeStorageConfig } from "../../api/gen/gastrolog/v1/storage_pb";
import { encode, encodeString } from "../../api/glid";
import { DEFAULT_STORAGE_CLASS } from "./wizardStorage";

const NODE_IDS = ["06gsor0000000000000000000a", "06gsor0000000000000000000b", "06gsor0000000000000000000c", "06gsor0000000000000000000d"];
const GENERATED = "brisk-otter";

function emptyCluster(storages: NodeStorageConfig[] = []) {
  return new GetSystemResponse({
    nodeConfigs: NODE_IDS.map((id, i) => ({ id: encodeString(id), name: `node-${i + 1}` })),
    nodeStorageConfigs: storages,
  });
}

interface StorageCall {
  config: { nodeId: Uint8Array; fileStorages: { path: string; name: string; storageClass: number }[] };
}
interface VaultCall {
  config: { type: VaultType; storageClass: number; name: string };
}

function storageCalls(): StorageCall[] {
  return m(mocks.systemClient, "setNodeStorageConfig").mock.calls.map((c) => c[0] as StorageCall);
}
function vaultCalls(): VaultCall[] {
  return m(mocks.systemClient, "putVault").mock.calls.map((c) => c[0] as VaultCall);
}

beforeEach(() => {
  for (const method of ["setNodeStorageConfig", "putVault", "putRoute", "putIngester", "putSetupSettings", "generateName"]) {
    m(mocks.systemClient, method).mockReset();
    m(mocks.systemClient, method).mockResolvedValue({});
  }
  m(mocks.systemClient, "generateName").mockResolvedValue({ name: GENERATED });
});

async function runWizard(system: GetSystemResponse, vault: { type?: "file" | "memory"; dir?: string } = {}) {
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], system);
  const ui = render(<SetupWizard />, { wrapper: settingsWrapper(qc) });

  fireEvent.click(ui.getByText("Get Started"));
  await ui.findByPlaceholderText(GENERATED);
  if (vault.type) {
    fireEvent.change(ui.getByRole("combobox"), { target: { value: vault.type } });
  }
  if (vault.dir !== undefined) {
    fireEvent.change(await ui.findByPlaceholderText(`storage/${GENERATED}`), { target: { value: vault.dir } });
  }
  for (let i = 0; i < 3; i++) fireEvent.click(ui.getByText("Next"));
  fireEvent.click(ui.getByText("Chatterbox"));
  fireEvent.click(ui.getByText("Next"));
  fireEvent.click(ui.getByText("Create"));
  await waitFor(() => expect(m(mocks.systemClient, "putSetupSettings")).toHaveBeenCalled());
  return ui;
}

describe("SetupWizard on a cluster with no storage", () => {
  test("a file vault gets a storage on every node before the vault is created, in the vault's class", async () => {
    await runWizard(emptyCluster());

    const calls = storageCalls();
    expect(calls.map((c) => encode(c.config.nodeId)).sort()).toEqual([...NODE_IDS].sort());
    for (const c of calls) {
      expect(c.config.fileStorages).toHaveLength(1);
      expect(c.config.fileStorages[0]!.path).toBe(`storage/${GENERATED}`);
      expect(c.config.fileStorages[0]!.storageClass).toBe(DEFAULT_STORAGE_CLASS);
    }

    const [vault] = vaultCalls();
    expect(vault!.config.type).toBe(VaultType.FILE);
    expect(vault!.config.storageClass).toBe(DEFAULT_STORAGE_CLASS);

    const lastStorage = Math.max(...m(mocks.systemClient, "setNodeStorageConfig").mock.invocationCallOrder);
    const vaultOrder = m(mocks.systemClient, "putVault").mock.invocationCallOrder[0]!;
    expect(lastStorage).toBeLessThan(vaultOrder);
  });

  test("the directory the operator types is the storage path on every node", async () => {
    await runWizard(emptyCluster(), { dir: "/var/lib/gastrolog" });

    const paths = storageCalls().map((c) => c.config.fileStorages[0]!.path);
    expect(paths).toEqual(NODE_IDS.map(() => "/var/lib/gastrolog"));
  });

  test("the review step shows the storage directory the vault will use", async () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], emptyCluster());
    const ui = render(<SetupWizard />, { wrapper: settingsWrapper(qc) });
    fireEvent.click(ui.getByText("Get Started"));
    fireEvent.change(await ui.findByPlaceholderText(`storage/${GENERATED}`), { target: { value: "/srv/logs" } });
    for (let i = 0; i < 3; i++) fireEvent.click(ui.getByText("Next"));
    fireEvent.click(ui.getByText("Chatterbox"));
    fireEvent.click(ui.getByText("Next"));

    expect(ui.getByText("Storage Directory")).toBeTruthy();
    expect(ui.getByText("/srv/logs")).toBeTruthy();
  });

  test("a memory vault is created as a memory vault and provisions no storage", async () => {
    await runWizard(emptyCluster(), { type: "memory" });

    expect(storageCalls()).toHaveLength(0);
    const [vault] = vaultCalls();
    expect(vault!.config.type).toBe(VaultType.MEMORY);
    expect(vault!.config.storageClass).toBe(0);
  });
});

describe("SetupWizard on a cluster that already has storage", () => {
  test("existing storages survive and a node already at the path is not written", async () => {
    const [a] = NODE_IDS as [string];
    const existing = new NodeStorageConfig({
      nodeId: encodeString(a),
      fileStorages: [
        new FileStorage({ id: crypto.getRandomValues(new Uint8Array(16)), name: "fast", path: "/mnt/fast", storageClass: 3 }),
      ],
    });
    await runWizard(emptyCluster([existing]), { dir: "/mnt/fast" });

    const written = storageCalls().map((c) => encode(c.config.nodeId));
    expect(written).not.toContain(a);
    expect(written).toHaveLength(NODE_IDS.length - 1);
    for (const c of storageCalls()) expect(c.config.fileStorages[0]!.storageClass).toBe(3);
    expect(vaultCalls()[0]!.config.storageClass).toBe(3);
  });
});

describe("SetupWizard on a cluster whose storage class pools other directories", () => {
  test("a new directory gets its own class, so the vault lands in it", async () => {
    const [a] = NODE_IDS as [string];
    const pooled = new NodeStorageConfig({
      nodeId: encodeString(a),
      fileStorages: [new FileStorage({ id: crypto.getRandomValues(new Uint8Array(16)), name: "cli", path: "storage/disk-1", storageClass: 1 })],
    });
    await runWizard(emptyCluster([pooled]), { dir: "storage/typed-dir" });

    const cls = vaultCalls()[0]!.config.storageClass;
    expect(cls).not.toBe(1);
    expect(storageCalls()).toHaveLength(NODE_IDS.length);
    for (const c of storageCalls()) {
      const added = c.config.fileStorages.at(-1)!;
      expect(added.path).toBe("storage/typed-dir");
      expect(added.storageClass).toBe(cls);
    }
  });

  test("an existing directory whose class is shared is refused before anything is written", async () => {
    const [a, b] = NODE_IDS as [string, string];
    const shared = [a, b].map((id, i) =>
      new NodeStorageConfig({
        nodeId: encodeString(id),
        fileStorages: [new FileStorage({ id: crypto.getRandomValues(new Uint8Array(16)), name: `s${i}`, path: i === 0 ? "/data/logs" : "/data/other", storageClass: 1 })],
      }),
    );
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], emptyCluster(shared));
    const ui = render(<SetupWizard />, { wrapper: settingsWrapper(qc) });
    fireEvent.click(ui.getByText("Get Started"));
    fireEvent.change(await ui.findByPlaceholderText(`storage/${GENERATED}`), { target: { value: "/data/logs" } });
    for (let i = 0; i < 3; i++) fireEvent.click(ui.getByText("Next"));
    fireEvent.click(ui.getByText("Chatterbox"));
    fireEvent.click(ui.getByText("Next"));
    fireEvent.click(ui.getByText("Create"));

    await waitFor(() => expect(ui.getByText(/shares its storage class/)).toBeTruthy());
    expect(storageCalls()).toHaveLength(0);
    expect(vaultCalls()).toHaveLength(0);
  });
});

describe("SetupWizard failure", () => {
  test("a failed storage write creates no vault", async () => {
    m(mocks.systemClient, "setNodeStorageConfig").mockRejectedValue(new Error("not leader"));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], emptyCluster());
    const ui = render(<SetupWizard />, { wrapper: settingsWrapper(qc) });
    fireEvent.click(ui.getByText("Get Started"));
    for (let i = 0; i < 3; i++) fireEvent.click(ui.getByText("Next"));
    fireEvent.click(ui.getByText("Chatterbox"));
    fireEvent.click(ui.getByText("Next"));
    fireEvent.click(ui.getByText("Create"));

    await waitFor(() => expect(ui.getByText("not leader")).toBeTruthy());
    expect(vaultCalls()).toHaveLength(0);
    expect(m(mocks.systemClient, "putSetupSettings")).not.toHaveBeenCalled();
  });
});
