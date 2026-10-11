import { describe, test, expect, beforeEach, mock } from "bun:test";
import { fireEvent, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

const mocks = installMockClients();

import { PoliciesSettings } from "./PoliciesSettings";
import { GetSystemResponse, RotationPolicyConfig, VaultConfig, VaultType } from "../../api/gen/gastrolog/v1/system_pb";
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

describe("PoliciesSettings labels", () => {
  // The size bound is "Max Size" on the rotation form as on the retention
  // form, matching the proto field and the CLI flag.
  test("the size bound is labelled Max Size on the add form and on a policy card", async () => {
    const { getByText, findByText, queryByText, getAllByText } = renderPolicies("weekly");
    fireEvent.click(getByText("Add Policy"));
    expect(await findByText("Set at least one condition")).toBeTruthy();
    expect(getByText("Max Size")).toBeTruthy();
    expect(queryByText("Max Bytes")).toBeNull();

    fireEvent.click(getByText("daily"));
    expect(getAllByText("Max Size").length).toBe(2);
  });
});

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

    // The query cache notifies its observers on a macrotask; flush it.
    await act(async () => {
      qc.setQueryData(["system"], sampleConfig);
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    expect(queryByText('A rotation policy named "default" already exists')).toBeNull();
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

const usedByConfig = {
  rotationPolicies: [new RotationPolicyConfig({ id: testId(20), name: "daily", maxAge: "24h" })],
  retentionPolicies: [],
  vaults: [
    new VaultConfig({ id: testId(1), name: "vault-alpha", type: VaultType.FILE, rotationPolicyId: testId(20) }),
    new VaultConfig({ id: testId(2), name: "vault-beta", type: VaultType.MEMORY }),
  ],
  routes: [],
  ingesters: [],
  nodeConfigs: [],
};

describe("PoliciesSettings used by", () => {
  test("a vault named in used by opens that vault on the Vaults tab", () => {
    const qc = createTestQueryClient();
    qc.setQueryData(["system"], usedByConfig);
    const navigate = mock((_tab: string, _entity?: string) => {});

    const { getByText, queryByText } = renderWritable(<PoliciesSettings dark onNavigateTo={navigate} />, {
      wrapper: settingsWrapper(qc),
    });

    expect(queryByText("vault-beta")).toBeNull();
    fireEvent.click(getByText("vault-alpha"));
    expect(navigate).toHaveBeenCalledWith("vaults", "vault-alpha");
  });
});
