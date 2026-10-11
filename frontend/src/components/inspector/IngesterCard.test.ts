import { describe, expect, test } from "bun:test";
import { ingesterStatusBadge, nodeChipTitle } from "./ingesterStatus";

// The status badge separates the three states the old "stopped" pill
// folded together: disabled, nowhere eligible to run, and not running on
// the nodes that are eligible.
describe("ingesterStatusBadge", () => {
  test("a disabled ingester says so, whatever its nodes report", () => {
    const spec = ingesterStatusBadge({ enabled: false, selected: 3, running: 3, variant: "muted" });
    expect(spec.text).toBe("disabled");
    expect(spec.title).toContain("enable it");
  });

  test("an enabled ingester with no eligible live node names that cause, not stopped", () => {
    const spec = ingesterStatusBadge({ enabled: true, selected: 0, running: 0, variant: "muted" });
    expect(spec.text).toBe("no eligible node");
    expect(spec.title).toContain("pin a live node");
  });

  test("a running count names its noun and its denominator", () => {
    expect(ingesterStatusBadge({ enabled: true, selected: 3, running: 3, variant: "info" }).text).toBe("3/3 running");
    expect(ingesterStatusBadge({ enabled: true, selected: 3, running: 3, variant: "info" }).title).toContain(
      "every eligible node",
    );
  });

  test("a partial count says the rest have not started it; a dead pin says the node is unreachable", () => {
    expect(ingesterStatusBadge({ enabled: true, selected: 3, running: 2, variant: "warn" }).title).toContain(
      "have not started it",
    );
    expect(ingesterStatusBadge({ enabled: true, selected: 3, running: 2, variant: "error" }).title).toContain(
      "unreachable",
    );
  });
});

describe("nodeChipTitle", () => {
  test("each chip colour has its state in words", () => {
    expect(nodeChipTitle("node-1", { alive: true, dead: false })).toBe("node-1: running the ingester");
    expect(nodeChipTitle("node-1", { alive: false, dead: false })).toBe(
      "node-1: eligible but not running the ingester",
    );
    expect(nodeChipTitle("node-1", { alive: false, dead: true })).toContain("unreachable");
  });
});
