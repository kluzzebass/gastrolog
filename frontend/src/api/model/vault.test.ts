import { describe, test, expect } from "bun:test";
import { VaultInfo } from "../gen/gastrolog/v1/vault_pb";
import { VaultConfig, VaultType } from "../gen/gastrolog/v1/system_pb";
import { Vault } from "./vault";
import { type EntityID, EMPTY_ID, idFromBytes } from "./id";

function idBytes(b: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(16));
  out[0] = b;
  return out;
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
  test("returns explicit nodeId when set", () => {
    const info = new VaultInfo({ id: idBytes(30), nodeId: idBytes(2) });
    const v = new Vault(info, null);
    expect(v.placementNodeId(NODE_LOCAL)).toBe(NODE_OTHER);
  });

  test("falls back to localFallback when nodeId is zero", () => {
    const info = new VaultInfo({ id: idBytes(31) });
    const v = new Vault(info, null);
    expect(v.placementNodeId(NODE_LOCAL)).toBe(NODE_LOCAL);
  });
});

describe("Vault.isOn", () => {
  test("true for the explicit placement node", () => {
    const info = new VaultInfo({ id: idBytes(40), nodeId: idBytes(2) });
    expect(new Vault(info, null).isOn(NODE_OTHER, NODE_LOCAL)).toBe(true);
  });

  test("false for a different node", () => {
    const info = new VaultInfo({ id: idBytes(41), nodeId: idBytes(2) });
    expect(new Vault(info, null).isOn(NODE_LOCAL, NODE_LOCAL)).toBe(false);
  });

  test("implicit-local vault matches the localFallback", () => {
    const info = new VaultInfo({ id: idBytes(42) });
    expect(new Vault(info, null).isOn(NODE_LOCAL, NODE_LOCAL)).toBe(true);
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
