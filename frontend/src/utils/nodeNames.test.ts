import { describe, test, expect } from "bun:test";
import { buildNodeNameMap, resolveNodeName } from "./nodeNames";

describe("buildNodeNameMap", () => {
  test("maps node ID to name", () => {
    const map = buildNodeNameMap([
      { id: "n1", name: "web-1" },
      { id: "n2", name: "web-2" },
    ]);
    expect(map.get("n1")).toBe("web-1");
    expect(map.get("n2")).toBe("web-2");
  });

  test("falls back to ID when name is empty", () => {
    const map = buildNodeNameMap([{ id: "n1", name: "" }]);
    expect(map.get("n1")).toBe("n1");
  });

  test("handles empty array", () => {
    const map = buildNodeNameMap([]);
    expect(map.size).toBe(0);
  });

  test("last entry wins for duplicate IDs", () => {
    const map = buildNodeNameMap([
      { id: "n1", name: "first" },
      { id: "n1", name: "second" },
    ]);
    expect(map.get("n1")).toBe("second");
  });
});

describe("resolveNodeName", () => {
  const map = buildNodeNameMap([
    { id: "n1", name: "web-1" },
    { id: "n2", name: "" },
  ]);

  test("returns name for known node", () => {
    expect(resolveNodeName(map, "n1")).toBe("web-1");
  });

  test("returns ID for node with empty name (normalized at build time)", () => {
    expect(resolveNodeName(map, "n2")).toBe("n2");
  });

  test("returns raw ID for unknown node", () => {
    expect(resolveNodeName(map, "n-unknown")).toBe("n-unknown");
  });
});
