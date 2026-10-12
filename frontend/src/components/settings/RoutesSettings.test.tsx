import { describe, test, expect, beforeEach } from "bun:test";
import { render, fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { RoutesSettings } from "./RoutesSettings";
import { displayExpression } from "./routeDisplay";
import { encode } from "../../api/glid";
import { renderWritable } from "../../testing/renderWritable";
import { HelpProvider } from "../../hooks/useHelp";
import { VaultType } from "../../api/gen/gastrolog/v1/system_pb";
import { mock } from "bun:test";

describe("route destinations eligibility", () => {
  function renderWithHelp(vaults: unknown[]) {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { ...sampleConfig, vaults });
    const openHelp = mock((_helpRef?: string) => {});
    const result = renderWritable(
      <HelpProvider onOpen={openHelp}>
        <RoutesSettings dark />
      </HelpProvider>,
      { wrapper: settingsWrapper(qc) },
    );
    return { ...result, openHelp };
  }

  test("a disabled vault is told apart from an enabled one, both selectable", () => {
    const { getByText, container } = renderWithHelp([
      { id: testId(20), name: "vault-alpha", type: VaultType.FILE, enabled: true },
      { id: testId(22), name: "vault-off", type: VaultType.MEMORY, enabled: false },
    ]);
    fireEvent.click(getByText("default-route"));
    expect(getByText("vault-alpha")).toBeTruthy();
    expect(getByText("vault-off — disabled, not accepting records")).toBeTruthy();
    const boxes = [...container.querySelectorAll('[role="checkbox"]')];
    expect(boxes.some((b) => b.getAttribute("aria-disabled") === "true")).toBe(false);
  });

  test("with no vault the destinations list names the cause and links to route components", () => {
    const { getByText, getByRole, openHelp } = renderWithHelp([]);
    fireEvent.click(getByText("default-route"));
    expect(getByRole("note").textContent).toContain("No vaults configured");
    fireEvent.click(getByText("Route components"));
    expect(openHelp).toHaveBeenCalledWith("routing#route-components");
  });
});

/** Create a distinct 16-byte Uint8Array test ID from a small number. */
function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

// Match expressions live inline on routes via Stages → MatchStage. The
// fixtures use the proto's oneof JSON shape.
const matchStage = (expression: string) => ({
  stage: { case: "match" as const, value: { expression } },
});

const sampleConfig = {
  routes: [
    {
      id: testId(1),
      name: "default-route",
      priority: 0,
      stages: [matchStage("env=prod")],
      // Two destinations so the distribution badge is meaningful here.
      destinations: [{ vaultId: testId(20) }, { vaultId: testId(21) }],
      distribution: "fanout",
      enabled: true,
    },
    {
      id: testId(2),
      name: "backup-route",
      priority: 0,
      stages: [matchStage("")],
      // Two destinations so the distribution badge stays visible.
      destinations: [{ vaultId: testId(20) }, { vaultId: testId(21) }],
      distribution: "failover",
      enabled: false,
    },
  ],
  vaults: [
    { id: testId(20), name: "vault-alpha" },
    { id: testId(21), name: "vault-beta" },
  ],
  ingesters: [],
  nodeConfigs: [],
};

beforeEach(() => {
  m(mocks.systemClient, "getConfig").mockClear();
  m(mocks.systemClient, "putRoute").mockClear();
  m(mocks.systemClient, "deleteRoute").mockClear();
  m(mocks.systemClient, "generateName").mockClear();
});

describe("RoutesSettings", () => {
  test("renders empty state when no routes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { routes: [], vaults: [], ingesters: [], nodeConfigs: [] });

    const { getByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/No routes configured/)).toBeTruthy();
  });

  test("renders route cards with names and distribution badges", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("default-route")).toBeTruthy();
    expect(getByText("backup-route")).toBeTruthy();
    expect(getByText("fanout")).toBeTruthy();
    expect(getByText("failover")).toBeTruthy();
  });

  // Distribution is only meaningful when there are 2+ destinations, so the
  // type badge that surfaces it should disappear for single-destination routes.
  test("hides distribution badge when route has a single destination", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], {
      ...sampleConfig,
      routes: [
        {
          id: testId(3),
          name: "solo-route",
          priority: 0,
          stages: [matchStage("*")],
          destinations: [{ vaultId: testId(20) }],
          distribution: "fanout",
          enabled: true,
        },
      ],
    });

    const { getByText, queryByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText("solo-route")).toBeTruthy();
    // No "fanout" / "round-robin" / "failover" badge should appear.
    expect(queryByText("fanout")).toBeNull();
    expect(queryByText("round-robin")).toBeNull();
    expect(queryByText("failover")).toBeNull();
  });

  test("shows match expression and destination names in route status", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText, getAllByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/env=prod/)).toBeTruthy();
    // Both routes in the fixture share the vault-alpha + vault-beta
    // destinations, so the destination string appears once per route.
    expect(getAllByText(/vault-alpha/).length).toBeGreaterThan(0);
  });

  test("shows disabled indicator for disabled routes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    expect(getByText(/\(disabled\)/)).toBeTruthy();
  });

  test("shows fallback label when route has no match expression", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getAllByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    // backup-route has an empty expression
    expect(getAllByText(/no match expression/).length).toBeGreaterThanOrEqual(1);
  });

  test("expands route card on click and shows edit form", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("default-route"));
    // Form fields should appear
    expect(getByText("Name")).toBeTruthy();
    expect(getByText("Match expression")).toBeTruthy();
    expect(getByText("Priority")).toBeTruthy();
    expect(getByText("Distribution")).toBeTruthy();
    expect(getByText("Destinations")).toBeTruthy();
    expect(getByText("Enabled")).toBeTruthy();
  });

  test("save button disabled when not dirty", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("default-route"));
    const saveBtn = getByText("Save").closest("button")!;
    expect(saveBtn.disabled).toBe(true);
  });

  test("deletes route via confirm flow", async () => {
    m(mocks.systemClient, "deleteRoute").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);

    const { getByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("default-route"));
    fireEvent.click(getByText("Delete"));
    fireEvent.click(getByText("Yes"));

    await waitFor(() => {
      expect(m(mocks.systemClient, "deleteRoute")).toHaveBeenCalledTimes(1);
    });
  });

  test("opens add form via Add Route button", async () => {
    m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: "lucky-panda" });
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], { routes: [], vaults: [{ id: testId(20), name: "vault-alpha" }], ingesters: [], nodeConfigs: [] });

    const { getByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Route"));
    // Add form should show Create button and form fields
    await waitFor(() => {
      expect(getByText("Create")).toBeTruthy();
      expect(getByText("Name")).toBeTruthy();
      expect(getByText("Distribution")).toBeTruthy();
    });
  });
});

describe("route header expression", () => {
  const vaults = [{ id: "abc", name: "retention-source" }];

  test("a _vault comparison against a configured vault shows the vault's name", () => {
    expect(displayExpression('_source = "retention" AND _vault = "abc"', vaults)).toBe(
      '_source = "retention" AND _vault = "retention-source"',
    );
    expect(displayExpression('_vault=="abc"', vaults)).toBe('_vault=="retention-source"');
    expect(displayExpression('_vault != "abc"', vaults)).toBe('_vault != "retention-source"');
  });

  test("an ID no vault carries, and any other field, stay as written", () => {
    expect(displayExpression('_vault = "zzz"', vaults)).toBe('_vault = "zzz"');
    expect(displayExpression('_ingester = "abc"', vaults)).toBe('_ingester = "abc"');
    expect(displayExpression("*", vaults)).toBe("*");
  });

  test("the card header renders the vault name; the editor keeps the ID", () => {
    const vaultId = encode(testId(20));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], {
      ...sampleConfig,
      routes: [{ ...sampleConfig.routes[0]!, stages: [matchStage(`_source = "retention" AND _vault = "${vaultId}"`)] }],
    });
    const { getByText, getByDisplayValue, queryByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });
    expect(getByText(/_vault = "vault-alpha"/)).toBeTruthy();
    expect(queryByText(new RegExp(`_vault = "${vaultId}"`))).toBeNull();

    fireEvent.click(getByText("default-route"));
    expect(getByDisplayValue(`_source = "retention" AND _vault = "${vaultId}"`)).toBeTruthy();
  });

  test("an unknown vault ID in the header stays as written", () => {
    const unknownId = encode(testId(99));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], {
      ...sampleConfig,
      routes: [{ ...sampleConfig.routes[0]!, stages: [matchStage(`_vault = "${unknownId}"`)] }],
    });
    const { getByText } = renderWritable(<RoutesSettings dark />, { wrapper: settingsWrapper(qc) });
    expect(getByText(new RegExp(`_vault = "${unknownId}"`))).toBeTruthy();
  });
});

describe("RoutesSettings create gates", () => {
  // With no generated name the form falls back to "route", which a route of
  // that name takes.
  test("a name conflict disables Create and says which name is taken", () => {
    m(mocks.systemClient, "generateName").mockRejectedValueOnce(new Error("unavailable"));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], {
      ...sampleConfig,
      routes: [{ ...sampleConfig.routes[0]!, name: "route" }],
    });
    const { getByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Route"));

    expect(getByText('A route named "route" already exists')).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);
  });

  test("no destination disables Create with the reason, until one is picked", async () => {
    m(mocks.systemClient, "generateName").mockRejectedValueOnce(new Error("unavailable"));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);
    const { getByText, queryByText } = renderWritable(<RoutesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("Add Route"));

    expect(getByText("Pick at least one destination")).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    // The destination label carries the vault's state after a dash.
    fireEvent.click(getByText(/^vault-alpha/));
    await waitFor(() => {
      expect(queryByText("Pick at least one destination")).toBeNull();
      expect(getByText("Create").closest("button")!.disabled).toBe(false);
    });
  });
});
