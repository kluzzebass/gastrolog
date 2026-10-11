import { describe, expect, test } from "bun:test";
import { entityCountTitle } from "./InspectorDialog";

// A nav count names what it counts on hover: "System 4" is four live
// nodes, "Jobs 7" is seven jobs active or scheduled.
describe("entityCountTitle", () => {
  test("names the noun for every entity type", () => {
    expect(entityCountTitle("system", 4)).toBe("4 live nodes");
    expect(entityCountTitle("jobs", 7)).toBe("7 jobs active or scheduled");
    expect(entityCountTitle("vaults", 2)).toBe("2 vaults configured");
    expect(entityCountTitle("storages", 4)).toBe("4 file storages");
    expect(entityCountTitle("ingesters", 3)).toBe("3 ingesters configured");
    expect(entityCountTitle("routes", 5)).toBe("5 routes configured");
  });

  test("a count of one takes the singular", () => {
    expect(entityCountTitle("system", 1)).toBe("1 live node");
    expect(entityCountTitle("jobs", 1)).toBe("1 job active or scheduled");
  });
});
