import { describe, test, expect, mock } from "bun:test";
import { render, fireEvent } from "@testing-library/react";
import type { ReactNode } from "react";
import { QueryClientProvider } from "@tanstack/react-query";
import { createTestQueryClient } from "../../../test/render";
import { HelpProvider } from "../../hooks/useHelp";
import { ToastProvider } from "../Toast";
import { StorageState } from "../../api/gen/gastrolog/v1/storage_pb";
import { VaultInfo, VaultPipelineBacklog } from "../../api/gen/gastrolog/v1/vault_pb";
import { VaultConfig } from "../../api/gen/gastrolog/v1/system_pb";
import { NodeStats } from "../../api/gen/gastrolog/v1/cluster_pb";
import { Storage } from "../../api/model/storage";
import { Vault } from "../../api/model/vault";
import { idFromBytes } from "../../api/model/id";
import { InspectorDialog } from "./InspectorDialog";
import { StorageCard } from "./StorageCard";
import { VaultCard } from "./VaultCard";
import { NodeDetailPane } from "./NodeDetailPane";
import { ClusterSummaryView } from "./SystemStatsView";

// Every help affordance in the inspector opens the help dialog on the
// section that explains the control: these tests pin the exact reference
// each control sends, so a renamed heading or a retargeted anchor fails
// here rather than landing the operator on a topic root.

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const NODE_A = testId(40);
const NODE_B = testId(41);
const VAULT = testId(1);
const vaultId = idFromBytes(VAULT);

function seedCluster(qc: ReturnType<typeof createTestQueryClient>, vaults: VaultConfig[] = []) {
  qc.setQueryData(["settings"], { nodeId: NODE_A });
  qc.setQueryData(["system"], {
    nodeConfigs: [
      { id: NODE_A, name: "node-1" },
      { id: NODE_B, name: "node-2" },
    ],
    vaults,
    ingesters: [],
    routes: [],
    nodeStorageConfigs: [],
    cloudServices: [],
  });
  qc.setQueryData(["clusterStatus"], {
    clusterEnabled: true,
    nodes: [
      { id: NODE_A, name: "node-1", isLeader: true, stats: new NodeStats() },
      { id: NODE_B, name: "node-2", stats: new NodeStats() },
    ],
  });
  qc.setQueryData(["vaults"], []);
  qc.setQueryData(["storages"], []);
}

function renderWithHelp(ui: ReactNode, qc = createTestQueryClient()) {
  const openHelp = mock((_helpRef?: string) => {});
  const result = render(
    <QueryClientProvider client={qc}>
      <HelpProvider onOpen={openHelp}>
        <ToastProvider dark={true}>{ui}</ToastProvider>
      </HelpProvider>
    </QueryClientProvider>,
  );
  return { ...result, openHelp };
}

function helpButtonBeside(label: HTMLElement): HTMLElement {
  const scope = label.closest("h2, h3, span, div");
  const button =
    label.querySelector<HTMLElement>("button[aria-label='Help']") ??
    scope?.parentElement?.querySelector<HTMLElement>("button[aria-label='Help']");
  if (!button) throw new Error(`no help button beside ${label.textContent}`);
  return button;
}

describe("inspector help anchors", () => {
  test("the Inspector nav heading opens the two-mode layout section", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const { getByText, openHelp } = renderWithHelp(
      <InspectorDialog dark isAdmin inspectorParam="entities:vaults" onNavigate={() => {}} onClose={() => {}} />,
      qc,
    );
    fireEvent.click(helpButtonBeside(getByText("Inspector")));
    expect(openHelp).toHaveBeenCalledWith("inspector#two-mode-layout");
  });

  test("the node list's leader badge opens node roles without selecting the node", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const onNavigate = mock((_param: string) => {});
    const { getByText, openHelp } = renderWithHelp(
      <InspectorDialog dark isAdmin inspectorParam="nodes:" onNavigate={onNavigate} onClose={() => {}} />,
      qc,
    );
    fireEvent.click(getByText("leader"));
    expect(openHelp).toHaveBeenCalledWith("clustering#node-roles");
    expect(onNavigate).not.toHaveBeenCalled();
  });

  test("a storage's class badge opens file storage without toggling the card", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const onToggle = mock(() => {});
    const storage = new Storage(
      new StorageState({ id: testId(2), name: "fast", nodeId: NODE_A, storageClass: 1, protectVerdict: true }),
    );
    const { getByText, openHelp } = renderWithHelp(
      <StorageCard storage={storage} dark expanded onToggle={onToggle} />,
      qc,
    );
    fireEvent.click(getByText("class 1"));
    expect(openHelp).toHaveBeenCalledWith("storage-config#file-storage");
    fireEvent.click(getByText("protected"));
    expect(openHelp).toHaveBeenCalledWith("inspector-storages#storage-overview");
    fireEvent.click(helpButtonBeside(getByText("Thresholds")));
    expect(openHelp).toHaveBeenCalledWith("storage-config#file-storage");
    expect(onToggle).not.toHaveBeenCalled();
  });

  test("a vault's Topology header opens the topology section", () => {
    const qc = createTestQueryClient();
    const config = new VaultConfig({ id: VAULT, name: "events", replicationFactor: 1, enabled: true });
    seedCluster(qc, [config]);
    qc.setQueryData(["chunks", vaultId], []);
    qc.setQueryData(["chunks-contribution", vaultId], null);
    qc.setQueryData(["pipeline-backlog", vaultId], new VaultPipelineBacklog());
    const vault = new Vault(new VaultInfo({ id: VAULT, name: "events", type: "file", enabled: true }), config);
    const { getByText, openHelp } = renderWithHelp(
      <VaultCard vault={vault} dark expanded onToggle={() => {}} />,
      qc,
    );
    fireEvent.click(helpButtonBeside(getByText("Topology")));
    expect(openHelp).toHaveBeenCalledWith("inspector-vaults#topology");
  });

  test("a node's Network section opens the network section", () => {
    const qc = createTestQueryClient();
    seedCluster(qc);
    const { getByText, openHelp } = renderWithHelp(
      <NodeDetailPane nodeId={idFromBytes(NODE_A)} dark />,
      qc,
    );
    fireEvent.click(helpButtonBeside(getByText("Network")));
    expect(openHelp).toHaveBeenCalledWith("inspector-system#network");
    fireEvent.click(helpButtonBeside(getByText("Tasks")));
    expect(openHelp).toHaveBeenCalledWith("inspector-jobs#tasks");
  });

  test("a leaderless cluster summary links its Leader row to quorum loss; a led one does not", () => {
    const leaderless = renderWithHelp(
      <ClusterSummaryView nodes={[{ id: NODE_A, name: "node-1" }] as never} vaultStats={null} configuredVaults={0} dark />,
    );
    fireEvent.click(leaderless.getByText("Leader"));
    expect(leaderless.openHelp).toHaveBeenCalledWith("clustering#quorum-loss-read-only-mode");
    leaderless.unmount();

    const led = renderWithHelp(
      <ClusterSummaryView nodes={[{ id: NODE_A, name: "node-1", isLeader: true }] as never} vaultStats={null} configuredVaults={0} dark />,
    );
    expect(led.getByText("Leader").tagName).toBe("SPAN");
    expect(led.openHelp).not.toHaveBeenCalled();
  });
});
