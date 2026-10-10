import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { StorageState } from "../../api/gen/gastrolog/v1/storage_pb";
import { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { GetStatsResponse, VaultInfo, VaultStats } from "../../api/gen/gastrolog/v1/vault_pb";
import { EntityListPane } from "./EntityListPane";

// The storages entity tab renders a FLAT list, exactly the VaultsList
// shape — no node grouping, no group headers. The node is already a badge
// on each card (StorageCard renders NodeBadge), so a grouped presentation
// would only duplicate the per-node view (NodeDetailPane's Storages
// section) with nothing new to offer.

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const NODE_A = testId(40);
const NODE_B = testId(41);

function seedCluster(qc: ReturnType<typeof createTestQueryClient>) {
  qc.setQueryData(["settings"], { nodeId: NODE_A });
  qc.setQueryData(["system"], {
    nodeConfigs: [
      { id: NODE_A, name: "node-1" },
      { id: NODE_B, name: "node-2" },
    ],
    vaults: [],
    ingesters: [],
    routes: [],
    nodeStorageConfigs: [],
  });
  qc.setQueryData(["clusterStatus"], {
    clusterEnabled: true,
    nodes: [
      { id: NODE_A, name: "node-1" },
      { id: NODE_B, name: "node-2" },
    ],
  });
}

describe("EntityListPane storages (flat list)", () => {
  test("renders storages sorted by name, not grouped by node", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    qc.setQueryData(["storages"], [
      new StorageState({ id: testId(2), name: "zebra-storage", nodeId: NODE_A }),
      new StorageState({ id: testId(1), name: "alpha-storage", nodeId: NODE_B }),
    ]);

    const { container } = render(<EntityListPane entityType="storages" dark />, {
      wrapper: settingsWrapper(qc),
    });

    // ExpandableCard renders its `id` prop as a `title` attribute on the
    // header span. In the old grouped presentation, the node name ("node-1")
    // was that `id` — a group header. Flat, only storage names get that
    // treatment; node names appear solely as NodeBadge pills (no title
    // attribute), so their absence here is the "not grouped" assertion.
    expect(container.querySelector("[title='node-1']")).toBeNull();
    expect(container.querySelector("[title='node-2']")).toBeNull();

    const titles = [...container.querySelectorAll("[title='alpha-storage'], [title='zebra-storage']")]
      .map((el) => el.getAttribute("title"));
    expect(titles).toEqual(["alpha-storage", "zebra-storage"]);
  });

  test("every card shows its owning node as a badge — single-node and multi-node render the same shape", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    qc.setQueryData(["storages"], [
      new StorageState({ id: testId(1), name: "fast-a", nodeId: NODE_A }),
      new StorageState({ id: testId(2), name: "fast-b", nodeId: NODE_B }),
    ]);

    const { getByText } = render(<EntityListPane entityType="storages" dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("fast-a")).toBeTruthy();
    expect(getByText("fast-b")).toBeTruthy();
    // Node badges, one per card — not a single shared group header.
    expect(getByText("node-1")).toBeTruthy();
    expect(getByText("node-2")).toBeTruthy();
    // The local node's card additionally gets the "this node" pill.
    expect(getByText("this node")).toBeTruthy();
  });

  test("empty storage list renders the same empty state as an empty vault list", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    qc.setQueryData(["storages"], []);

    const { getByText } = render(<EntityListPane entityType="storages" dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/No storages configured/)).toBeTruthy();
  });
});

// The value text of the stat row whose label reads `label`.
function statRow(container: HTMLElement, label: string): string | null {
  const labelEl = [...container.querySelectorAll("span")].find((el) => el.textContent === label);
  return labelEl?.parentElement?.textContent.slice(label.length) ?? null;
}

describe("EntityListPane system cluster summary", () => {
  const VAULT_A = testId(1);
  const VAULT_B = testId(2);
  const nodeIds = [testId(40), testId(41), testId(42), testId(43)];

  // Each node broadcasts its own copy of every vault it is a home for: vault
  // A sits on all four nodes, vault B on none yet.
  function seedReplicatedCluster(qc: ReturnType<typeof createTestQueryClient>, vaultStats: GetStatsResponse | null) {
    qc.setQueryData(["settings"], { nodeId: nodeIds[0] });
    qc.setQueryData(["system"], {
      nodeConfigs: nodeIds.map((id, i) => ({ id, name: `node-${i + 1}` })),
      vaults: [],
      ingesters: [],
      routes: [],
      nodeStorageConfigs: [],
    });
    qc.setQueryData(["clusterStatus"], {
      clusterEnabled: true,
      nodes: nodeIds.map((id, i) => ({
        id,
        name: `node-${i + 1}`,
        isLeader: i === 1,
        stats: new NodeStats({
          vaults: [
            new VaultStats({ id: VAULT_A, recordCount: 607_073n, chunkCount: 61n, dataBytes: 161_000_000n }),
            new VaultStats({ id: VAULT_B }),
          ],
        }),
      })),
    });
    qc.setQueryData(["vaults"], [new VaultInfo({ id: VAULT_A, name: "a" }), new VaultInfo({ id: VAULT_B, name: "b" })]);
    if (vaultStats) qc.setQueryData(["stats", "all"], vaultStats);
  }

  test("shows each vault's holdings once, not once per replica", () => {
    const qc = createTestQueryClient();
    seedReplicatedCluster(
      qc,
      new GetStatsResponse({ totalVaults: 2n, totalRecords: 607_073n, totalChunks: 61n, totalBytes: 1024n }),
    );

    const { container } = render(<EntityListPane entityType="system" dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(statRow(container, "Vaults")).toBe("2");
    expect(statRow(container, "Records")).toBe((607_073).toLocaleString());
    expect(statRow(container, "Chunks")).toBe("61");
    expect(statRow(container, "Data")).toBe("1.0 KiB");
    expect(container.textContent).not.toContain("cover");
  });

  test("shows unknown holdings, not node copies, before the cluster stats arrive", () => {
    const qc = createTestQueryClient();
    seedReplicatedCluster(qc, null);

    const { container } = render(<EntityListPane entityType="system" dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(statRow(container, "Vaults")).toBe("2");
    expect(statRow(container, "Records")).toBe("—");
    expect(statRow(container, "Chunks")).toBe("—");
    expect(statRow(container, "Data")).toBe("—");
  });

  test("says which share of the vaults the totals cover when a vault has no figures", () => {
    const qc = createTestQueryClient();
    seedReplicatedCluster(
      qc,
      new GetStatsResponse({ totalVaults: 1n, totalRecords: 607_073n, totalChunks: 61n, totalBytes: 1024n }),
    );

    const { container, getByText } = render(<EntityListPane entityType="system" dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(statRow(container, "Vaults")).toBe("2");
    expect(statRow(container, "Records")).toBe((607_073).toLocaleString());
    expect(getByText("Records, data, and chunks cover 1 of 2 vaults.")).toBeTruthy();
  });
});
