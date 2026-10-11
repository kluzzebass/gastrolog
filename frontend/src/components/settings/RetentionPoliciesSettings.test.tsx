import { describe, test, expect, mock } from "bun:test";
import { fireEvent } from "@testing-library/react";
import { installMockClients } from "../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";

installMockClients();

import { RetentionPoliciesSettings } from "./RetentionPoliciesSettings";
import { RetentionPolicyConfig, VaultConfig, VaultType } from "../../api/gen/gastrolog/v1/system_pb";
import { renderWritable } from "../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const config = {
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
    qc.setQueryData(["system"], config);
    const navigate = mock((_tab: string, _entity?: string) => {});

    const { getByText, queryByText } = renderWritable(<RetentionPoliciesSettings dark onNavigateTo={navigate} />, {
      wrapper: settingsWrapper(qc),
    });

    expect(queryByText("vault-beta")).toBeNull();
    fireEvent.click(getByText("vault-alpha"));
    expect(navigate).toHaveBeenCalledWith("vaults", "vault-alpha");
  });
});
