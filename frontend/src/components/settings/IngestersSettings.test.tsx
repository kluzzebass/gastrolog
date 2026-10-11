import { describe, test, expect, beforeEach } from "bun:test";
import { fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { IngestersSettings, ingesterModeBadge } from "./IngestersSettings";
import { GetSystemResponse, IngesterMode } from "../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const sampleConfig = new GetSystemResponse({
  ingesters: [
    // Off the :514 example chip, so a new syslog ingester set from the chip does not clash with it.
    { id: testId(1), name: "edge-syslog", type: "syslog", enabled: true, params: { udp_addr: ":5514" }, allNodes: true },
    { id: testId(2), name: "bare-relp", type: "relp", enabled: true, params: {}, allNodes: true },
  ],
  nodeConfigs: [{ id: testId(40), name: "node-1" }],
});

beforeEach(() => {
  m(mocks.systemClient, "generateName").mockClear();
});

function renderIngesters(generatedName = "fresh-ingester") {
  m(mocks.systemClient, "generateName").mockResolvedValue({ name: generatedName });
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], sampleConfig);
  qc.setQueryData(["ingesterDefaults"], { defaults: {}, modes: {}, singletonSupported: {} });
  return renderWritable(<IngestersSettings dark />, { wrapper: settingsWrapper(qc) });
}

describe("IngestersSettings type labels", () => {
  // The add menu and the card badges use the help topics' titles, so what
  // the operator picks is what the help calls it.
  test("the add menu and the type badges carry the help titles, not the raw type keys", () => {
    const { getByText, getAllByText, queryByText } = renderIngesters();
    expect(getByText("RELP")).toBeTruthy();
    expect(queryByText("relp")).toBeNull();

    fireEvent.click(getByText("Add Ingester"));
    for (const title of ["Fluent Forward", "OTLP", "HTTP (Loki)", "Kafka", "Chatterbox"]) {
      expect(getAllByText(title).some((el) => el.tagName === "BUTTON")).toBe(true);
    }
    expect(queryByText("fluentfwd")).toBeNull();
    expect(queryByText("otlp")).toBeNull();

    // Picking a type opens its add form, whose type badge carries the same title.
    fireEvent.click(getAllByText("Fluent Forward").find((el) => el.tagName === "BUTTON")!);
    expect(getByText("Create")).toBeTruthy();
    expect(getAllByText("Fluent Forward").some((el) => el.tagName !== "BUTTON")).toBe(true);
  });
});

describe("IngestersSettings create gates", () => {
  test("a missing required param disables Create with the field named, until it is set", async () => {
    const { getByText, findByText, getAllByText, queryByText } = renderIngesters();

    fireEvent.click(getByText("Add Ingester"));
    // The existing syslog card's type badge also says "Syslog"; the menu item is the button.
    fireEvent.click(getAllByText("Syslog").find((el) => el.tagName === "BUTTON")!);

    expect(await findByText("UDP Address or TCP Address is required")).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    // The UDP Address example chip sets the field.
    fireEvent.click(getAllByText(":514")[0]!);
    await waitFor(() => {
      expect(queryByText("UDP Address or TCP Address is required")).toBeNull();
      expect(getByText("Create").closest("button")!.disabled).toBe(false);
    });
  });

  // With no generated name the form falls back to the type name, which an
  // ingester called "chatterbox" takes.
  test("a name conflict disables Create and says which name is taken", () => {
    m(mocks.systemClient, "generateName").mockRejectedValue(new Error("unavailable"));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], new GetSystemResponse({
      ingesters: [{ id: testId(3), name: "chatterbox", type: "chatterbox", enabled: true, allNodes: true }],
    }));
    qc.setQueryData(["ingesterDefaults"], { defaults: {}, modes: {}, singletonSupported: {} });
    const { getByText, getAllByText } = renderWritable(<IngestersSettings dark />, { wrapper: settingsWrapper(qc) });

    fireEvent.click(getByText("Add Ingester"));
    fireEvent.click(getAllByText("Chatterbox").find((el) => el.tagName === "BUTTON")!);

    expect(getByText('An ingester named "chatterbox" already exists')).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);
  });
});

describe("IngestersSettings save gates", () => {
  test("a card missing a required param says so beside Save; a complete one does not", () => {
    const { getByText, queryByText } = renderIngesters();

    fireEvent.click(getByText("bare-relp"));
    expect(getByText("Listen Address is required")).toBeTruthy();
    expect(getByText("Save").closest("button")!.disabled).toBe(true);
    fireEvent.click(getByText("bare-relp"));

    fireEvent.click(getByText("edge-syslog"));
    expect(queryByText(/is required/)).toBeNull();
  });
});

describe("ingesterModeBadge", () => {
  test("a listener binds a port; an active ingester pulls", () => {
    expect(ingesterModeBadge(IngesterMode.PASSIVE)).toEqual({
      text: "listener",
      title: "listener — binds a port and accepts what clients push to it",
    });
    expect(ingesterModeBadge(IngesterMode.ACTIVE).text).toBe("active");
    expect(ingesterModeBadge(IngesterMode.ACTIVE).title).toContain("pulls");
  });
});
