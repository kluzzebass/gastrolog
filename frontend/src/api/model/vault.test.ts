import { describe, test, expect } from "bun:test";
import { VaultInfo } from "../gen/gastrolog/v1/vault_pb";
import { VaultConfig, VaultPlacement, VaultType } from "../gen/gastrolog/v1/system_pb";
import { FileStorage, NodeStorageConfig } from "../gen/gastrolog/v1/storage_pb";
import { Vault } from "./vault";
import { encode } from "../glid";
import { type EntityID, EMPTY_ID, idFromBytes } from "./id";

function idBytes(b: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(16));
  out[0] = b;
  return out;
}

// Test fixture: three nodes, each with one file storage. Returns the NSC
// array plus typed handles for the test bodies to construct placements
// and assert against resolved node IDs.
function clusterFixture() {
  const nodeA = idBytes(1);
  const nodeB = idBytes(2);
  const nodeC = idBytes(3);
  const storageA = idBytes(11);
  const storageB = idBytes(12);
  const storageC = idBytes(13);

  const nscs: NodeStorageConfig[] = [
    new NodeStorageConfig({ nodeId: nodeA, fileStorages: [new FileStorage({ id: storageA })] }),
    new NodeStorageConfig({ nodeId: nodeB, fileStorages: [new FileStorage({ id: storageB })] }),
    new NodeStorageConfig({ nodeId: nodeC, fileStorages: [new FileStorage({ id: storageC })] }),
  ];

  return {
    nscs,
    nodeA: encode(nodeA) as EntityID,
    nodeB: encode(nodeB) as EntityID,
    nodeC: encode(nodeC) as EntityID,
    storageA,
    storageB,
    storageC,
  };
}

const NODE_LOCAL: EntityID = idFromBytes(idBytes(1));
const NODE_OTHER: EntityID = idFromBytes(idBytes(2));

describe("Vault.name fallback", () => {
  test("prefers VaultInfo.name", () => {
    const info = new VaultInfo({ id: idBytes(10), name: "runtime-name" });
    const config = new VaultConfig({ id: idBytes(10), name: "config-name" });
    expect(new Vault(info, config).name).toBe("runtime-name");
  });

  test("falls back to VaultConfig.name when runtime is empty", () => {
    const info = new VaultInfo({ id: idBytes(11), name: "" });
    const config = new VaultConfig({ id: idBytes(11), name: "config-name" });
    expect(new Vault(info, config).name).toBe("config-name");
  });

  test("falls back to id when both names are empty", () => {
    const info = new VaultInfo({ id: idBytes(12), name: "" });
    const v = new Vault(info, null);
    expect(v.name).toBe(v.id);
  });
});

describe("Vault.isCloudBacked", () => {
  test("true when VaultConfig has a non-zero cloudServiceId", () => {
    const config = new VaultConfig({ id: idBytes(20), cloudServiceId: idBytes(99) });
    expect(new Vault(null, config).isCloudBacked).toBe(true);
  });

  test("false when cloudServiceId is empty/zero", () => {
    const config = new VaultConfig({ id: idBytes(21) });
    expect(new Vault(null, config).isCloudBacked).toBe(false);
  });

  test("false when no config is joined", () => {
    const info = new VaultInfo({ id: idBytes(22) });
    expect(new Vault(info, null).isCloudBacked).toBe(false);
  });
});

describe("Vault.placementNodeId", () => {
  test("returns the leader node when placements have one", () => {
    const f = clusterFixture();
    const config = new VaultConfig({
      id: idBytes(30),
      placements: [
        new VaultPlacement({ storageId: f.storageB, leader: true }),
        new VaultPlacement({ storageId: f.storageA }),
        new VaultPlacement({ storageId: f.storageC }),
      ],
    });
    expect(new Vault(null, config).placementNodeId(f.nscs, NODE_LOCAL)).toBe(f.nodeB);
  });

  test("falls back to localFallback when no placements are registered", () => {
    const f = clusterFixture();
    const config = new VaultConfig({ id: idBytes(31) }); // no placements
    expect(new Vault(null, config).placementNodeId(f.nscs, NODE_LOCAL)).toBe(NODE_LOCAL);
  });

  test("falls back when placements exist but none is leader (degraded state)", () => {
    const f = clusterFixture();
    // Mid-failover: leader gone, followers waiting. The model must not
    // pick an arbitrary follower as the answer — the fallback wins.
    const config = new VaultConfig({
      id: idBytes(32),
      placements: [
        new VaultPlacement({ storageId: f.storageA }),
        new VaultPlacement({ storageId: f.storageB }),
      ],
    });
    expect(new Vault(null, config).placementNodeId(f.nscs, NODE_LOCAL)).toBe(NODE_LOCAL);
  });
});

describe("Vault.placementNodeIds", () => {
  test("returns leader + followers for a fully-placed vault", () => {
    const f = clusterFixture();
    const config = new VaultConfig({
      id: idBytes(35),
      placements: [
        new VaultPlacement({ storageId: f.storageB, leader: true }),
        new VaultPlacement({ storageId: f.storageA }),
        new VaultPlacement({ storageId: f.storageC }),
      ],
    });
    const ids = new Vault(null, config).placementNodeIds(f.nscs, EMPTY_ID);
    expect(ids.toSorted()).toEqual([f.nodeA, f.nodeB, f.nodeC].toSorted());
  });

  test("returns just followers when leader is absent (degraded but still placed)", () => {
    const f = clusterFixture();
    const config = new VaultConfig({
      id: idBytes(36),
      placements: [
        new VaultPlacement({ storageId: f.storageA }),
        new VaultPlacement({ storageId: f.storageC }),
      ],
    });
    const ids = new Vault(null, config).placementNodeIds(f.nscs, EMPTY_ID);
    expect(ids.toSorted()).toEqual([f.nodeA, f.nodeC].toSorted());
  });

  test("returns [localFallback] for a freshly created vault with no placements", () => {
    const f = clusterFixture();
    const config = new VaultConfig({ id: idBytes(37) });
    expect(new Vault(null, config).placementNodeIds(f.nscs, NODE_LOCAL)).toEqual([NODE_LOCAL]);
  });

  test("returns [] when there are no placements and no localFallback", () => {
    const f = clusterFixture();
    const config = new VaultConfig({ id: idBytes(38) });
    expect(new Vault(null, config).placementNodeIds(f.nscs, EMPTY_ID)).toEqual([]);
  });
});

describe("Vault.isOn", () => {
  // The inspector list filter MUST count followers as "on this node" too,
  // otherwise non-leader pods show "No vaults on this node" while serving
  // real chunk replicas.
  test("true for a follower placement", () => {
    const f = clusterFixture();
    const config = new VaultConfig({
      id: idBytes(40),
      placements: [
        new VaultPlacement({ storageId: f.storageA, leader: true }),
        new VaultPlacement({ storageId: f.storageB }),
        new VaultPlacement({ storageId: f.storageC }),
      ],
    });
    const v = new Vault(null, config);
    expect(v.isOn(f.nodeB, f.nscs, EMPTY_ID)).toBe(true);
    expect(v.isOn(f.nodeC, f.nscs, EMPTY_ID)).toBe(true);
  });

  test("true for the leader placement", () => {
    const f = clusterFixture();
    const config = new VaultConfig({
      id: idBytes(41),
      placements: [new VaultPlacement({ storageId: f.storageA, leader: true })],
    });
    expect(new Vault(null, config).isOn(f.nodeA, f.nscs, EMPTY_ID)).toBe(true);
  });

  test("false for nodes outside the placement set", () => {
    const f = clusterFixture();
    const config = new VaultConfig({
      id: idBytes(42),
      placements: [new VaultPlacement({ storageId: f.storageA, leader: true })],
    });
    expect(new Vault(null, config).isOn(f.nodeB, f.nscs, EMPTY_ID)).toBe(false);
    expect(new Vault(null, config).isOn(f.nodeC, f.nscs, EMPTY_ID)).toBe(false);
  });

  test("pre-placement vaults still match the local fallback (memory vaults, new vaults)", () => {
    const f = clusterFixture();
    const config = new VaultConfig({ id: idBytes(43) });
    expect(new Vault(null, config).isOn(NODE_LOCAL, f.nscs, NODE_LOCAL)).toBe(true);
    expect(new Vault(null, config).isOn(NODE_OTHER, f.nscs, NODE_LOCAL)).toBe(false);
  });

  test("ignores the legacy info.nodeId field even when set (the field is dead)", () => {
    // The backend stopped populating VaultInfo.nodeId when placements
    // landed; preserving compat would mean re-introducing the bug.
    const f = clusterFixture();
    const info = new VaultInfo({ id: idBytes(44), nodeId: idBytes(99) });
    const config = new VaultConfig({
      id: idBytes(44),
      placements: [new VaultPlacement({ storageId: f.storageA, leader: true })],
    });
    expect(new Vault(info, config).isOn(f.nodeA, f.nscs, EMPTY_ID)).toBe(true);
    // node 99 from info.nodeId must NOT match.
    expect(new Vault(info, config).isOn(idFromBytes(idBytes(99)), f.nscs, EMPTY_ID)).toBe(false);
  });
});

describe("Vault.enabled", () => {
  test("prefers VaultConfig.enabled when available", () => {
    const info = new VaultInfo({ id: idBytes(50), enabled: true });
    const config = new VaultConfig({ id: idBytes(50), enabled: false });
    expect(new Vault(info, config).enabled).toBe(false);
  });

  test("falls back to VaultInfo.enabled when no config", () => {
    const info = new VaultInfo({ id: idBytes(51), enabled: true });
    expect(new Vault(info, null).enabled).toBe(true);
  });
});

describe("Vault.configType", () => {
  test("returns the typed enum from VaultConfig", () => {
    const config = new VaultConfig({ id: idBytes(60), type: VaultType.FILE });
    expect(new Vault(null, config).configType).toBe(VaultType.FILE);
  });

  test("returns UNSPECIFIED when no config is joined", () => {
    expect(new Vault(new VaultInfo({ id: idBytes(61) }), null).configType).toBe(VaultType.UNSPECIFIED);
  });
});

describe("Vault id edge case", () => {
  test("EMPTY_ID when no source is provided", () => {
    expect(new Vault(null, null).id).toBe(EMPTY_ID);
  });
});
