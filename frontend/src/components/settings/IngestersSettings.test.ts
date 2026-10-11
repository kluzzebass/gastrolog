import { describe, expect, test } from "bun:test";
import { IngesterMode } from "../../api/gen/gastrolog/v1/system_pb";
import { ingesterModeBadge, ingesterNodesBadge } from "./IngestersSettings";

describe("ingesterModeBadge", () => {
  test("a listener binds a port; an active ingester pulls", () => {
    expect(ingesterModeBadge(IngesterMode.PASSIVE)).toEqual({
      text: "listener",
      title: "listener — binds a port and accepts what clients push to it",
    });
    expect(ingesterModeBadge(IngesterMode.ACTIVE).text).toBe("active");
    expect(ingesterModeBadge(IngesterMode.ACTIVE).title).toContain("pulls");
  });
});

// Eligibility is AllNodes/NodeIDs alone, the rule the dispatcher applies;
// the mode plays no part, so a listener pinned to nodes is not "all nodes".
describe("ingesterNodesBadge", () => {
  test("all nodes, and an empty pin list, read all nodes with the reason", () => {
    expect(ingesterNodesBadge(0, true)?.text).toBe("all nodes");
    expect(ingesterNodesBadge(2, true)?.title).toContain("join later");
    expect(ingesterNodesBadge(0, false)).toEqual({
      text: "all nodes",
      title: "no pinned nodes — eligible on every cluster node",
    });
  });

  test("several pins count the nodes; one pin leaves the node's own badge to render", () => {
    expect(ingesterNodesBadge(3, false)).toEqual({ text: "3 nodes", title: "eligible on its 3 pinned nodes only" });
    expect(ingesterNodesBadge(1, false)).toBeNull();
  });
});
