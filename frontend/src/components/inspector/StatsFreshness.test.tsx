import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { Timestamp } from "@bufbuild/protobuf";
import { StatsFreshnessCue } from "./StatsFreshness";
import { statsFreshness } from "./sampleFreshness";

// A broadcast value is shown with its age, from the responding node's
// receipt instant, and warns once the sample is older than the stats TTL —
// the cluster has stopped serving it, so the figure on screen is a copy
// nothing refreshes.

const NOW = 1_700_000_000_000;
const TTL_MS = 15_000;

function receivedAgo(ms: number): Timestamp {
  return Timestamp.fromDate(new Date(NOW - ms));
}

describe("statsFreshness", () => {
  test("no sample yields no cue rather than a zero age", () => {
    expect(statsFreshness(undefined, TTL_MS, NOW)).toBeNull();
  });

  test("a sample within the TTL is dated and not stale", () => {
    const fresh = statsFreshness(receivedAgo(3_000), TTL_MS, NOW);
    expect(fresh).not.toBeNull();
    expect(fresh!.stale).toBe(false);
  });

  test("a sample older than the TTL is stale", () => {
    expect(statsFreshness(receivedAgo(TTL_MS + 1_000), TTL_MS, NOW)!.stale).toBe(true);
  });

  test("an unknown TTL never calls a sample stale", () => {
    expect(statsFreshness(receivedAgo(10 * TTL_MS), null, NOW)!.stale).toBe(false);
  });
});

describe("StatsFreshnessCue", () => {
  test("a fresh sample reads as sampled N ago in the muted tone", () => {
    const { container } = render(
      <StatsFreshnessCue receivedAt={Timestamp.fromDate(new Date(Date.now() - 2_000))} ttlMs={TTL_MS} dark />,
    );
    expect(container.textContent).toMatch(/^sampled \d+s ago$/);
    expect(container.querySelector(".text-severity-warn")).toBeNull();
  });

  test("a sample past the TTL is marked stale in the warn tone", () => {
    const { container } = render(
      <StatsFreshnessCue receivedAt={Timestamp.fromDate(new Date(Date.now() - 2 * TTL_MS))} ttlMs={TTL_MS} dark />,
    );
    expect(container.textContent).toContain("stale");
    expect(container.querySelector(".text-severity-warn")).not.toBeNull();
  });

  test("renders nothing without a sample", () => {
    const { container } = render(<StatsFreshnessCue receivedAt={undefined} ttlMs={TTL_MS} dark />);
    expect(container.textContent).toBe("");
  });
});
