import { describe, test, expect } from "bun:test";
import { RouteConfig, PerRouteStats } from "../gen/gastrolog/v1/system_pb";
import { Route } from "./route";

function bytes(b: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(16));
  out[0] = b;
  return out;
}

describe("Route", () => {
  test("id, name, enabled, priority come from config", () => {
    const cfg = new RouteConfig({ id: bytes(1), name: "r1", priority: 5, enabled: true });
    const r = new Route(cfg, null);
    expect(r.name).toBe("r1");
    expect(r.priority).toBe(5);
    expect(r.enabled).toBe(true);
    expect(r.id).not.toBe("");
  });

  test("displayLabel falls back to id when name is empty", () => {
    const cfg = new RouteConfig({ id: bytes(2), name: "" });
    const r = new Route(cfg, null);
    expect(r.displayLabel).toBe(r.id);
  });

  test("recordsMatched comes from stats when present", () => {
    const cfg = new RouteConfig({ id: bytes(3) });
    const stats = new PerRouteStats({ routeId: bytes(3), recordsMatched: 100n });
    const r = new Route(cfg, stats);
    expect(r.recordsMatched).toBe(100n);
  });

  test("recordsMatched is 0 when stats are absent", () => {
    const cfg = new RouteConfig({ id: bytes(4) });
    const r = new Route(cfg, null);
    expect(r.recordsMatched).toBe(0n);
  });
});
