import { describe, expect, test, afterEach } from "bun:test";
import { render, cleanup } from "@testing-library/react";
import { Spark } from "./Spark";

afterEach(cleanup);

function renderSpark(values: readonly number[], width?: number, height?: number) {
  const { container } = render(<Spark values={values} width={width} height={height} />);
  const svg = container.querySelector("svg");
  if (!svg) throw new Error("Spark rendered no svg");
  return { svg, polyline: svg.querySelector("polyline") };
}

function yCoords(points: string): number[] {
  return points
    .trim()
    .split(" ")
    .map((p) => Number(p.split(",")[1]));
}

describe("Spark", () => {
  test("renders an empty box with no samples", () => {
    const { svg, polyline } = renderSpark([]);
    expect(polyline).toBeNull();
    expect(svg.getAttribute("width")).toBe("56");
    expect(svg.getAttribute("height")).toBe("16");
  });

  test("renders an empty box with a single sample", () => {
    expect(renderSpark([5]).polyline).toBeNull();
  });

  // An idle series must read as idle: a baseline stroke next to "0.0/s"
  // looks like a rendering glitch, not like quiet.
  test("renders an empty box for an all-zero series", () => {
    expect(renderSpark([0, 0]).polyline).toBeNull();
    expect(renderSpark([0, 0, 0, 0, 0, 0, 0, 0]).polyline).toBeNull();
  });

  test("keeps the requested box size for an all-zero series", () => {
    const { svg } = renderSpark([0, 0, 0], 40, 12);
    expect(svg.getAttribute("width")).toBe("40");
    expect(svg.getAttribute("height")).toBe("12");
  });

  // A steady non-zero rate is information, distinct from idle.
  test("draws a level line for a constant non-zero series", () => {
    const { polyline } = renderSpark([7, 7, 7, 7]);
    expect(polyline).not.toBeNull();
    const ys = yCoords(polyline?.getAttribute("points") ?? "");
    expect(ys).toHaveLength(4);
    expect(new Set(ys).size).toBe(1);
  });

  test("draws a line when any sample in the window is non-zero", () => {
    const { polyline } = renderSpark([0, 0, 3, 0, 0]);
    expect(polyline).not.toBeNull();
    expect(yCoords(polyline?.getAttribute("points") ?? "")).toHaveLength(5);
  });

  test("draws a line for a sub-unit non-zero series", () => {
    expect(renderSpark([0.2, 0.2, 0.2]).polyline).not.toBeNull();
  });

  test("scales the peak sample to the top and zero to the baseline", () => {
    const { polyline } = renderSpark([0, 10, 5], 20, 10);
    expect(polyline?.getAttribute("points")).toBe("0.0,10.0 10.0,0.0 20.0,5.0");
  });
});
