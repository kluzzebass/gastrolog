import { describe, test, expect } from "bun:test";
import { chartReducer, CHART_INITIAL } from "./useHistogramInteraction";

describe("chartReducer", () => {
  test("hover sets group and bar", () => {
    const state = chartReducer(CHART_INITIAL, { type: "hover", group: "error", bar: 3 });
    expect(state.hoveredGroup).toBe("error");
    expect(state.hoveredBar).toBe(3);
  });

  test("hover clears on null", () => {
    const hovered = chartReducer(CHART_INITIAL, { type: "hover", group: "error", bar: 3 });
    const cleared = chartReducer(hovered, { type: "hover", group: null, bar: null });
    expect(cleared.hoveredGroup).toBeNull();
    expect(cleared.hoveredBar).toBeNull();
  });

  test("brushStart sets both start and end to same index", () => {
    const state = chartReducer(CHART_INITIAL, { type: "brushStart", idx: 5 });
    expect(state.brushStart).toBe(5);
    expect(state.brushEnd).toBe(5);
  });

  test("brushMove updates end only", () => {
    const started = chartReducer(CHART_INITIAL, { type: "brushStart", idx: 2 });
    const moved = chartReducer(started, { type: "brushMove", idx: 7 });
    expect(moved.brushStart).toBe(2);
    expect(moved.brushEnd).toBe(7);
  });

  test("brushEnd clears both", () => {
    const started = chartReducer(CHART_INITIAL, { type: "brushStart", idx: 2 });
    const ended = chartReducer(started, { type: "brushEnd" });
    expect(ended.brushStart).toBeNull();
    expect(ended.brushEnd).toBeNull();
  });

  test("panStart sets axis width", () => {
    const state = chartReducer(CHART_INITIAL, { type: "panStart", axisWidth: 400 });
    expect(state.panAxisWidth).toBe(400);
  });

  test("panMove sets offset", () => {
    const started = chartReducer(CHART_INITIAL, { type: "panStart", axisWidth: 400 });
    const moved = chartReducer(started, { type: "panMove", offset: -50 });
    expect(moved.panOffset).toBe(-50);
  });

  test("panEnd resets offset to 0", () => {
    const moved = chartReducer(
      chartReducer(CHART_INITIAL, { type: "panStart", axisWidth: 400 }),
      { type: "panMove", offset: -50 },
    );
    const ended = chartReducer(moved, { type: "panEnd" });
    expect(ended.panOffset).toBe(0);
    expect(ended.panAxisWidth).toBe(400); // axisWidth preserved
  });

  test("hover preserves brush state", () => {
    const brushed = chartReducer(CHART_INITIAL, { type: "brushStart", idx: 3 });
    const hovered = chartReducer(brushed, { type: "hover", group: "info", bar: 5 });
    expect(hovered.brushStart).toBe(3);
    expect(hovered.hoveredGroup).toBe("info");
  });
});
