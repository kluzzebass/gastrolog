import { describe, test, expect, beforeEach } from "bun:test";
import { fireEvent, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { PoliciesSettings } from "./PoliciesSettings";
import { GetSystemResponse } from "../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const dailyPolicy = { id: testId(1), name: "daily", maxSize: "", maxRecords: BigInt(0), maxAge: "24h", cron: "" };

// Real messages: the system query compares writes with the proto's own
// equals, which needs every repeated field present.
const sampleInit = {
  rotationPolicies: [dailyPolicy],
  vaults: [
    { id: testId(10), name: "vault-alpha", rotationPolicyId: testId(1) },
    { id: testId(11), name: "vault-beta", rotationPolicyId: testId(1) },
    { id: testId(12), name: "vault-gamma" },
  ],
};
const sampleConfig = new GetSystemResponse(sampleInit);

beforeEach(() => {
  m(mocks.systemClient, "generateName").mockClear();
});

function renderPolicies(generatedName: string) {
  m(mocks.systemClient, "generateName").mockResolvedValueOnce({ name: generatedName });
  const qc = createTestQueryClient();
  qc.setQueryData(["system"], sampleConfig);
  return { qc, ...renderWritable(<PoliciesSettings dark />, { wrapper: settingsWrapper(qc) }) };
}

describe("PoliciesSettings create gates", () => {
  // With no generated name the form falls back to "default", which a policy
  // of that name takes; the clash goes when that policy is renamed.
  test("a name conflict disables Create and says which name is taken, until the clash goes", async () => {
    m(mocks.systemClient, "generateName").mockRejectedValueOnce(new Error("unavailable"));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], new GetSystemResponse({ ...sampleInit, rotationPolicies: [{ ...dailyPolicy, name: "default" }] }));
    const { getByText, queryByText } = renderWritable(<PoliciesSettings dark />, { wrapper: settingsWrapper(qc) });
    fireEvent.click(getByText("Add Policy"));

    expect(getByText('A rotation policy named "default" already exists')).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    act(() => {
      qc.setQueryData(["system"], sampleConfig);
    });
    await waitFor(() => {
      expect(queryByText('A rotation policy named "default" already exists')).toBeNull();
    });
  });

  test("no condition disables Create with the reason, until one is set", async () => {
    const { getByText, findByText, getAllByText, queryByText } = renderPolicies("weekly");
    fireEvent.click(getByText("Add Policy"));

    expect(await findByText("Set at least one condition")).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    // The Max Age example chip sets the field.
    fireEvent.click(getAllByText("7d")[0]!);
    await waitFor(() => {
      expect(queryByText("Set at least one condition")).toBeNull();
      expect(getByText("Create").closest("button")!.disabled).toBe(false);
    });
  });
});

describe("PoliciesSettings delete consequences", () => {
  test("confirming a delete names the vaults that lose the policy, before Yes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);
    const { getByText, queryByText } = renderWritable(<PoliciesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("daily"));
    fireEvent.click(getByText("Delete"));

    const line = getByText(/rotation policy cleared/);
    expect(line.textContent).toContain("vault-alpha, vault-beta");
    expect(line.textContent).not.toContain("vault-gamma");
    expect(getByText("Yes")).toBeTruthy();
    expect(queryByText("Delete")).toBeNull();
  });

  test("a policy nothing uses lists no consequence", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], new GetSystemResponse({ ...sampleInit, vaults: [] }));
    const { getByText, queryByText } = renderWritable(<PoliciesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("daily"));
    fireEvent.click(getByText("Delete"));

    expect(queryByText(/rotation policy cleared/)).toBeNull();
    expect(getByText("Yes")).toBeTruthy();
  });
});
