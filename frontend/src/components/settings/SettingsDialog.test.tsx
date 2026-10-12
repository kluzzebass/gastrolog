import { describe, test, expect, beforeEach, afterEach, spyOn } from "bun:test";
import { useEffect, useState } from "react";
import { act, fireEvent, render, within } from "@testing-library/react";
import { installMockClients } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

installMockClients();

import { SettingsDialog } from "./SettingsDialog";
import { GetSystemResponse, VaultConfig, VaultType } from "../../api/gen/gastrolog/v1/system_pb";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const sampleConfig = new GetSystemResponse({
  ingesters: [
    { id: testId(1), name: "edge-syslog", type: "syslog", enabled: true, params: { udp_addr: ":5514" }, allNodes: true },
    { id: testId(2), name: "bare-relp", type: "relp", enabled: true, params: {}, allNodes: true },
  ],
  vaults: [
    new VaultConfig({ id: testId(10), name: "vault-alpha", enabled: true, type: VaultType.MEMORY, replicationFactor: 1 }),
    new VaultConfig({ id: testId(11), name: "vault-beta", enabled: true, type: VaultType.MEMORY, replicationFactor: 1 }),
  ],
  nodeConfigs: [{ id: testId(40), name: "node-1" }],
});

let setUrlTab: (tab: string) => void = () => {};

/** Owns the `settings` URL param the way SearchView does. */
function DialogAtUrl({ initialTab }: Readonly<{ initialTab: string }>) {
  const [tab, setTab] = useState(initialTab);
  useEffect(() => {
    setUrlTab = setTab;
  }, []);
  return <SettingsDialog dark tab={tab} onTabChange={setTab} onClose={() => {}} isAdmin />;
}

function renderDialog(initialTab: string) {
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], sampleConfig);
  qc.setQueryData(["ingesterDefaults"], { defaults: {}, modes: {}, singletonSupported: {} });
  return render(<DialogAtUrl initialTab={initialTab} />, { wrapper: settingsWrapper(qc) });
}

const page = () => within(document.body);
const cardExpanded = (name: string) =>
  page().getByText(name).closest("[aria-expanded]")?.getAttribute("aria-expanded") === "true";
const openTab = (label: string) => {
  fireEvent.click(page().getByText(label, { selector: "nav button span" }));
};

let consoleError: ReturnType<typeof spyOn>;

beforeEach(() => {
  consoleError = spyOn(console, "error");
});

afterEach(() => {
  consoleError.mockRestore();
});

describe("SettingsDialog deep-link expansion", () => {
  test.each([
    ["ingesters", "edge-syslog", "bare-relp"],
    ["vaults", "vault-alpha", "vault-beta"],
  ])("a %s deep link expands its card without updating the dialog from the tab's render", (tab, target, other) => {
    renderDialog(`${tab}:${target}`);

    expect(cardExpanded(target)).toBe(true);
    expect(cardExpanded(other)).toBe(false);
    const renderPhaseUpdates = consoleError.mock.calls.filter((args: unknown[]) =>
      String(args[0]).includes("Cannot update a component"),
    );
    expect(renderPhaseUpdates).toEqual([]);
  });

  test("leaving the tab and coming back does not expand the card again", () => {
    renderDialog("ingesters:edge-syslog");
    fireEvent.click(page().getByText("edge-syslog"));
    expect(cardExpanded("edge-syslog")).toBe(false);

    openTab("Routes");
    openTab("Ingesters");

    expect(cardExpanded("edge-syslog")).toBe(false);
  });

  test("a new deep link on the open tab expands that card", () => {
    renderDialog("ingesters:edge-syslog");

    act(() => setUrlTab("ingesters:bare-relp"));

    expect(cardExpanded("bare-relp")).toBe(true);
  });

  test("a deep link for one tab does not expand a same-named card on another tab", () => {
    renderDialog("storage:edge-syslog");

    openTab("Ingesters");

    expect(cardExpanded("edge-syslog")).toBe(false);
  });
});
