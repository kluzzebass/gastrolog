import { describe, expect, test, afterEach } from "bun:test";
import { render, cleanup } from "@testing-library/react";
import type { Timestamp } from "../api/model/node";
import { OfflineBadge } from "./OfflineBadge";

afterEach(cleanup);

// Only the CURRENT instant is faked. The instant being measured FROM is the
// backend's ClusterNode.last_seen, which is the entire point of
// gastrolog-231eli: the browser may format elapsed time, but it must never be
// the source of the origin.
const NOW_MS = 1_700_000_000_000;
const NOW_SECS = BigInt(Math.floor(NOW_MS / 1000));

function renderAt(lastSeen: Timestamp | undefined, isOffline = true) {
  const origNow = Date.now;
  Date.now = () => NOW_MS;
  try {
    return render(<OfflineBadge lastSeen={lastSeen} isOffline={isOffline} dark />);
  } finally {
    Date.now = origNow;
  }
}

function secondsAgo(n: bigint): Timestamp {
  return { seconds: NOW_SECS - n } as Timestamp;
}

describe("OfflineBadge", () => {
  test("renders nothing when the node is not offline", () => {
    const { container } = renderAt(secondsAgo(30n), false);
    expect(container.firstChild).toBeNull();
  });

  test("shows the duration since the cluster last saw the node", () => {
    const { getByText } = renderAt(secondsAgo(25n));
    expect(getByText("offline 25s")).toBeTruthy();
  });

  // The defect this component was rebuilt out of: the duration used to start
  // when the browser tab first noticed, so a long-gone node read as seconds to
  // anyone who had just opened the panel. A freshly-mounted badge must report
  // the full absence.
  test("reports a long absence on a freshly-mounted badge", () => {
    const { getByText } = renderAt(secondsAgo(7_200n));
    expect(getByText("offline 2h")).toBeTruthy();
  });

  // Absent / zero last_seen is PeerState.LastSeen's deliberate "no positive
  // evidence has ever been observed" signal — claim nothing about duration.
  test("renders a bare label when the cluster has never seen the node", () => {
    const { getByText } = renderAt(undefined);
    expect(getByText("offline")).toBeTruthy();
  });

  test("renders a bare label for a zero timestamp", () => {
    const { getByText } = renderAt({ seconds: 0n } as Timestamp);
    expect(getByText("offline")).toBeTruthy();
  });

  test("renders a bare label when last seen is under a second ago", () => {
    const { getByText } = renderAt(secondsAgo(0n));
    expect(getByText("offline")).toBeTruthy();
  });

  // Clock skew between this browser and the cluster must not render a negative.
  test("renders a bare label when last seen is in the future", () => {
    const { getByText } = renderAt(secondsAgo(-10n));
    expect(getByText("offline")).toBeTruthy();
  });
});
