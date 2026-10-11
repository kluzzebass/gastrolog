import { describe, expect, test } from "bun:test";
import { render } from "@testing-library/react";
import { SealPips } from "./SealPips";
import { CALM_SEALED_CLASS, pipClass } from "./sealPipClass";
import { computePips, type PipInputs } from "./sealPipState";

// A sealed pip is the only pip drawn without a border, so its fill is all
// there is to see. These tests pin the two fills the design gives it: a
// calm grey-green mix when the whole row is healthy, full green beside an
// anomaly — never an alpha-only fill that fades into the ink surface.

const base: PipInputs = {
  chunkState: "sealed",
  placementNodes: ["node-1", "node-2", "node-3", "node-4"],
  residentNodes: ["node-1", "node-2", "node-3", "node-4"],
  pendingAckNodes: [],
  deleteInFlight: false,
  liveNodes: new Set(["node-1", "node-2", "node-3", "node-4"]),
};

function pipClasses(container: HTMLElement): string[] {
  return [...container.querySelectorAll("span[aria-label]")].map((el) => el.className);
}

describe("SealPips sealed fill", () => {
  test("a fully sealed, healthy row renders every pip with the calm mix, not an alpha fill", () => {
    const { pips, ghosts } = computePips(base);
    const { container } = render(<SealPips pips={pips} ghosts={ghosts} />);
    const classes = pipClasses(container);
    expect(classes).toHaveLength(4);
    for (const cls of classes) {
      expect(cls).toContain(CALM_SEALED_CLASS);
      expect(cls).not.toContain("bg-severity-info/");
    }
  });

  test("the calm mix blends the green with the muted text tone so it stays visible on the ink surface", () => {
    expect(CALM_SEALED_CLASS).toContain("color-mix(");
    expect(CALM_SEALED_CLASS).toContain("var(--color-severity-info)");
    expect(CALM_SEALED_CLASS).toContain("var(--color-text-muted)");
  });

  test("beside an unreachable node the sealed pips stay full green", () => {
    const { pips, ghosts } = computePips({ ...base, liveNodes: new Set(["node-1", "node-2", "node-3"]) });
    const { container } = render(<SealPips pips={pips} ghosts={ghosts} />);
    const classes = pipClasses(container);
    expect(classes.slice(0, 3).every((cls) => / bg-severity-info( |$)/.test(cls))).toBe(true);
    expect(classes.some((cls) => cls.includes(CALM_SEALED_CLASS))).toBe(false);
  });

  test("a ghost after the gap takes the row out of calm", () => {
    const { pips, ghosts } = computePips({ ...base, residentNodes: [...base.residentNodes, "node-9"] });
    expect(ghosts).toHaveLength(1);
    const { container } = render(<SealPips pips={pips} ghosts={ghosts} />);
    expect(pipClasses(container).some((cls) => cls.includes(CALM_SEALED_CLASS))).toBe(false);
  });

  test("a caller rendering one pip at a time passes the row's verdict through", () => {
    const { pips } = computePips({ ...base, liveNodes: new Set(["node-1"]) });
    const sealedOnly = pips.filter((p) => p.state === "sealed");
    const { container } = render(<SealPips pips={sealedOnly.slice(0, 1)} ghosts={[]} calm={false} />);
    expect(pipClasses(container)[0]).not.toContain(CALM_SEALED_CLASS);
  });

  test("pipClass dims only the sealed state", () => {
    expect(pipClass("sealed", true)).toBe(CALM_SEALED_CLASS);
    expect(pipClass("sealed", false)).toBe("bg-severity-info");
    expect(pipClass("uncached", true)).toBe(pipClass("uncached", false));
    expect(pipClass("missing", true)).toBe(pipClass("missing", false));
  });
});
