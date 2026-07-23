import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { StorageState } from "../../api/gen/gastrolog/v1/storage_pb";
import { EntityListPane } from "./EntityListPane";

// gastrolog-3cobq4: the storages entity tab renders a FLAT list, exactly
// the VaultsList shape — no node grouping, no group headers. The node is
// already a badge on each card (StorageCard renders NodeBadge), so a
// grouped presentation would only duplicate the per-node view
// (NodeDetailPane's Storages section) with nothing new to offer. These
// tests replace the deleted groupStoragesByNode grouping tests.

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
