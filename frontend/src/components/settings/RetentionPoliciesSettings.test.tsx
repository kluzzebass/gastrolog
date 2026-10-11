import { describe, test, expect, beforeEach, mock } from "bun:test";
import { fireEvent, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { RetentionPoliciesSettings } from "./RetentionPoliciesSettings";
import { GetSystemResponse, RetentionPolicyConfig, VaultConfig, VaultType } from "../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const thirtyDays = { id: testId(1), name: "30-day", maxAge: "30d", maxSize: "", maxChunks: BigInt(0), refuse: false };

// Real messages: the system query compares writes with the proto's own
// equals, which needs every repeated field present.
const sampleInit = {
  retentionPolicies: [thirtyDays],
  vaults: [
    { id: testId(10), name: "vault-alpha", retentionRules: [{ retentionPolicyId: testId(1) }] },
    { id: testId(11), name: "vault-beta", retentionRules: [] },
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
  return { qc, ...renderWritable(<RetentionPoliciesSettings dark />, { wrapper: settingsWrapper(qc) }) };
}

describe("RetentionPoliciesSettings create gates", () => {
  // With no generated name the form falls back to "default", which a policy
  // of that name takes; the clash goes when that policy is renamed.
  test("a name conflict disables Create and says which name is taken, until the clash goes", async () => {
    m(mocks.systemClient, "generateName").mockRejectedValueOnce(new Error("unavailable"));
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], new GetSystemResponse({ ...sampleInit, retentionPolicies: [{ ...thirtyDays, name: "default" }] }));
    const { getByText, queryByText } = renderWritable(<RetentionPoliciesSettings dark />, { wrapper: settingsWrapper(qc) });
    fireEvent.click(getByText("Add Policy"));

    expect(getByText('A retention policy named "default" already exists')).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    // The query cache notifies its observers on a macrotask; flush it.
    await act(async () => {
      qc.setQueryData(["system"], sampleConfig);
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    expect(queryByText('A retention policy named "default" already exists')).toBeNull();
  });

  test("no condition disables Create with the reason, until one is set", async () => {
    const { getByText, findByText, getAllByText, queryByText } = renderPolicies("90-day");
    fireEvent.click(getByText("Add Policy"));

    expect(await findByText("Set at least one condition")).toBeTruthy();
    expect(getByText("Create").closest("button")!.disabled).toBe(true);

    fireEvent.click(getAllByText("90d")[0]!);
    await waitFor(() => {
      expect(queryByText("Set at least one condition")).toBeNull();
      expect(getByText("Create").closest("button")!.disabled).toBe(false);
    });
  });
});

describe("RetentionPoliciesSettings delete consequences", () => {
  test("confirming a delete names the vaults that lose the policy, before Yes", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], sampleConfig);
    const { getByText } = renderWritable(<RetentionPoliciesSettings dark />, {
      wrapper: settingsWrapper(qc),
    });

    fireEvent.click(getByText("30-day"));
    fireEvent.click(getByText("Delete"));

    const line = getByText(/retention policy cleared/);
    expect(line.textContent).toContain("vault-alpha");
    expect(line.textContent).not.toContain("vault-beta");
    expect(getByText("Yes")).toBeTruthy();
  });
});

const usedByConfig = {
  rotationPolicies: [],
  retentionPolicies: [new RetentionPolicyConfig({ id: testId(30), name: "30-day", maxAge: "720h" })],
  vaults: [
    new VaultConfig({ id: testId(1), name: "vault-alpha", type: VaultType.FILE, retentionRules: [{ retentionPolicyId: testId(30) }] }),
    new VaultConfig({ id: testId(2), name: "vault-beta", type: VaultType.MEMORY }),
  ],
  routes: [],
  ingesters: [],
  nodeConfigs: [],
};

describe("RetentionPoliciesSettings used by", () => {
  test("a vault named in used by opens that vault on the Vaults tab", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], usedByConfig);
    const navigate = mock((_tab: string, _entity?: string) => {});

    const { getByText, queryByText } = renderWritable(<RetentionPoliciesSettings dark onNavigateTo={navigate} />, {
      wrapper: settingsWrapper(qc),
    });

    expect(queryByText("vault-beta")).toBeNull();
    fireEvent.click(getByText("vault-alpha"));
    expect(navigate).toHaveBeenCalledWith("vaults", "vault-alpha");
  });
});
