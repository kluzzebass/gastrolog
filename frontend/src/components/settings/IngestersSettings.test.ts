import { describe, expect, test } from "bun:test";
import { IngesterMode } from "../../api/gen/gastrolog/v1/system_pb";
import { ingesterModeBadge } from "./IngestersSettings";

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
