import { describe, expect, test } from "bun:test";
import { render } from "@testing-library/react";
import { Timestamp } from "@bufbuild/protobuf";
import { HelpProvider } from "../hooks/useHelp";
import { NodeState } from "../api/model/node";
import { NODE_STATES_HELP_REF, NodeStateBadge } from "./NodeStateBadge";
import { nodeStateView } from "./nodeStateView";
import { OfflineBadge } from "./OfflineBadge";

function withHelp(ui: React.ReactElement, onOpen: (ref?: string) => void = () => {}) {
  return render(<HelpProvider onOpen={onOpen}>{ui}</HelpProvider>);
}

// Every node-state badge says what the state means and what, if
// anything, to do; the live state shows nothing.
describe("nodeStateView", () => {
  test("live and unspecified show no badge", () => {
    expect(nodeStateView(NodeState.LIVE)).toBeUndefined();
    expect(nodeStateView(NodeState.UNSPECIFIED)).toBeUndefined();
  });

  test("each non-live state names its cause and consequence", () => {
    expect(nodeStateView(NodeState.UNREACHABLE)?.description).toContain("check the process");
    expect(nodeStateView(NodeState.MAINTENANCE)?.description).toContain("operator-declared");
  });
});

describe("NodeStateBadge and OfflineBadge help", () => {
  test("a state badge opens the node-states help section", () => {
    const opened: (string | undefined)[] = [];
    const { getByText } = withHelp(
      <NodeStateBadge state={NodeState.UNREACHABLE} dark />,
      (ref) => opened.push(ref),
    );
    getByText("unreachable").click();
    expect(opened).toEqual([NODE_STATES_HELP_REF]);
    expect(NODE_STATES_HELP_REF).toBe("clustering-nodes#node-states");
  });

  test("the offline badge says the node is down or unreachable and opens the same section", () => {
    const opened: (string | undefined)[] = [];
    const { getByText } = withHelp(
      <OfflineBadge lastSeen={new Timestamp({ seconds: 0n })} isOffline dark />,
      (ref) => opened.push(ref),
    );
    const badge = getByText("offline");
    expect(badge.getAttribute("title")).toContain("down or unreachable");
    badge.click();
    expect(opened).toEqual([NODE_STATES_HELP_REF]);
  });
});
