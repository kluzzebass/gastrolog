import { describe, expect, test } from "bun:test";
import React from "react";
import { render } from "@testing-library/react";
import { Timestamp } from "@bufbuild/protobuf";
import { NodeStateBadge } from "./NodeStateBadge";
import { NodeState } from "../api/model/node";

function stateSinceSecondsAgo(seconds: number): Timestamp {
  const now = Math.floor(Date.now() / 1000);
  return new Timestamp({ seconds: BigInt(now - seconds), nanos: 0 });
}

describe("NodeStateBadge", () => {
  test("renders nothing for LIVE state", () => {
    const { container } = render(
      <NodeStateBadge state={NodeState.LIVE} dark />,
    );
    expect(container.firstChild).toBeNull();
  });

  test("renders nothing for UNSPECIFIED state", () => {
    const { container } = render(
      <NodeStateBadge state={NodeState.UNSPECIFIED} dark />,
    );
    expect(container.firstChild).toBeNull();
  });

  test("renders 'unreachable' badge for UNREACHABLE state", () => {
    const { getByText } = render(
      <NodeStateBadge state={NodeState.UNREACHABLE} dark />,
    );
    expect(getByText("unreachable")).toBeTruthy();
  });

  test("renders 'maintenance' badge for MAINTENANCE state", () => {
    const { getByText } = render(
      <NodeStateBadge state={NodeState.MAINTENANCE} dark />,
    );
    expect(getByText("maintenance")).toBeTruthy();
  });

  test("renders 'draining' badge for DRAINING state", () => {
    const { getByText } = render(
      <NodeStateBadge state={NodeState.DRAINING} dark />,
    );
    expect(getByText("draining")).toBeTruthy();
  });

  test("renders 'decommissioning' badge for DECOMMISSIONING state", () => {
    const { getByText } = render(
      <NodeStateBadge state={NodeState.DECOMMISSIONING} dark />,
    );
    expect(getByText("decommissioning")).toBeTruthy();
  });

  test("appends duration when stateSince is recent", () => {
    const { getByText } = render(
      <NodeStateBadge
        state={NodeState.UNREACHABLE}
        stateSince={stateSinceSecondsAgo(120)}
        dark
      />,
    );
    expect(getByText("unreachable 2m")).toBeTruthy();
  });

  test("omits duration when stateSince has zero seconds (legacy record)", () => {
    const { getByText } = render(
      <NodeStateBadge
        state={NodeState.UNREACHABLE}
        stateSince={new Timestamp({ seconds: 0n, nanos: 0 })}
        dark
      />,
    );
    expect(getByText("unreachable")).toBeTruthy();
  });

  test("omits duration when stateSince is in the future (clock skew)", () => {
    const { getByText } = render(
      <NodeStateBadge
        state={NodeState.MAINTENANCE}
        stateSince={stateSinceSecondsAgo(-60)}
        dark
      />,
    );
    // Future timestamp → elapsed is negative → no duration appended.
    expect(getByText("maintenance")).toBeTruthy();
  });
});
