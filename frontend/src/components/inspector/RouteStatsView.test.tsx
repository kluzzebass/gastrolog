import { describe, test, expect } from "bun:test";
import { render } from "@testing-library/react";
import { createTestQueryClient, settingsWrapper } from "../../../test/render";
import { GetRouteStatsResponse, VaultRouteStats } from "../../api/gen/gastrolog/v1/system_pb";
import { RouteStatsView } from "./RouteStatsView";

function renderWith(vaultStats: VaultRouteStats[]) {
  const qc = createTestQueryClient();
  qc.setQueryData(["route-stats"], new GetRouteStatsResponse({ routeTableActive: true, vaultStats }));
  qc.setQueryData(["vaults"], []);
  return render(<RouteStatsView dark />, { wrapper: settingsWrapper(qc) });
}

// Records a route sent a vault's way that it did not receive — a disabled
// destination, or one whose sink went away — are shown, not hidden.
describe("RouteStatsView per-vault drops", () => {
  test("a vault with drops shows a Dropped column and its count", () => {
    const { container } = renderWith([
      new VaultRouteStats({ vaultId: new Uint8Array(16).fill(1), recordsMatched: BigInt(300), recordsDropped: BigInt(0) }),
      new VaultRouteStats({ vaultId: new Uint8Array(16).fill(2), recordsMatched: BigInt(0), recordsDropped: BigInt(200) }),
    ]);
    expect(container.textContent).toContain("Dropped");
    expect(container.textContent).toContain("200");
  });

  test("with no drops anywhere the column stays out of the way", () => {
    const { container } = renderWith([
      new VaultRouteStats({ vaultId: new Uint8Array(16).fill(1), recordsMatched: BigInt(300) }),
    ]);
    expect(container.textContent).not.toContain("Dropped");
  });
});
