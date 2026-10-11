import { describe, test, expect, mock } from "bun:test";
import { render, fireEvent, within } from "@testing-library/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { createTestQueryClient } from "../../../test/render";
import { HelpProvider } from "../../hooks/useHelp";
import { PipelineNodeSegments, VaultPipelineBacklog } from "../../api/gen/gastrolog/v1/vault_pb";
import { idFromBytes } from "../../api/model/id";
import {
  PipelineBacklogView,
  PIPELINE_BACKLOG_EMPTY_TEXT,
  PIPELINE_BACKLOG_HELP_REF,
} from "./PipelineBacklogView";

// Pins the panel's operator labels: every figure is named by what it counts
// (eligible / registry segments, staging areas by their canonical names,
// origins and homes by role), and the empty state names its source.

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const NODE_A = testId(40);
const NODE_B = testId(41);
const VAULT = testId(1);
const vaultId = idFromBytes(VAULT);

function nodeSegments(nodeId: Uint8Array<ArrayBuffer>, completed: number): PipelineNodeSegments {
  return new PipelineNodeSegments({
    nodeId,
    workingSegments: 1,
    completedStagingSegments: completed,
    preHeadSegments: 0,
    headSegments: 212,
    workingBytes: BigInt(104),
    completedStagingBytes: BigInt(600_000),
    preHeadBytes: BigInt(0),
    headBytes: BigInt(2_400_000),
  });
}

/** The live shape of a vault with a published registry and a filling manifest. */
function backlogFixture(): VaultPipelineBacklog {
  return new VaultPipelineBacklog({
    vaultId: VAULT,
    registrySegments: 212,
    eligibleSegments: 0,
    registryRecords: BigInt(7799),
    openManifestRefs: 212,
    openManifestRecords: BigInt(7799),
    workingSegments: 4,
    completedStagingSegments: 212,
    preHeadSegments: 0,
    headSegments: 848,
    workingBytes: BigInt(416),
    completedStagingBytes: BigInt(2_400_000),
    preHeadBytes: BigInt(0),
    headBytes: BigInt(9_700_000),
    nodeSegments: [nodeSegments(NODE_A, 53), nodeSegments(NODE_B, 51)],
  });
}

function renderPanel(backlog: VaultPipelineBacklog | null) {
  const qc = createTestQueryClient();
  qc.setQueryData(["settings"], { nodeId: NODE_A });
  qc.setQueryData(["system"], {
    nodeConfigs: [
      { id: NODE_A, name: "node-1" },
      { id: NODE_B, name: "node-2" },
    ],
    vaults: [],
    ingesters: [],
    routes: [],
    nodeStorageConfigs: [],
    cloudServices: [],
  });
  qc.setQueryData(["clusterStatus"], { clusterEnabled: true, nodes: [] });
  qc.setQueryData(["pipeline-backlog", vaultId], backlog);
  qc.setQueryData(["pipeline-backlog-contribution", vaultId], null);
  const openHelp = mock((_helpRef?: string) => {});
  const result = render(
    <QueryClientProvider client={qc}>
      <HelpProvider onOpen={openHelp}>
        <PipelineBacklogView vaultId={vaultId} dark />
      </HelpProvider>
    </QueryClientProvider>,
  );
  return { ...result, openHelp };
}

function headerTexts(table: HTMLElement): string[] {
  return [...table.querySelectorAll("thead th")].map((th) => th.textContent);
}

function tableUnder(container: HTMLElement, heading: string): HTMLElement {
  const h3 = [...container.querySelectorAll("h3")].find((el) => el.textContent.trim() === heading);
  if (!h3) throw new Error(`no heading ${heading}`);
  const table = h3.parentElement?.querySelector("table");
  if (!table) throw new Error(`no table under ${heading}`);
  return table;
}

describe("PipelineBacklogView labels", () => {
  test("the headline names eligible / registry segments and spells out the open manifest", () => {
    const { container, getByText } = renderPanel(backlogFixture());
    const headline = container.querySelector("table");
    expect(headline).not.toBeNull();
    expect(headerTexts(headline!)).toEqual([
      "Eligible / registry segments",
      "Registry records",
      "Open manifest",
      "Oldest unchunked ingest",
    ]);
    expect(getByText("0 / 212")).toBeDefined();
    expect(getByText("7.8K")).toBeDefined();
    expect(getByText("212 segments · 7.8K records")).toBeDefined();
  });

  test("staging areas are listed under a Staging area column by their canonical names, in pipeline order", () => {
    const { container } = renderPanel(backlogFixture());
    const table = tableUnder(container, "Staging areas (cluster total)");
    expect(headerTexts(table)).toEqual(["Staging area", "Segments", "Bytes"]);
    const areas = [...table.querySelectorAll("tbody tr td:first-child")].map((td) => td.textContent);
    expect(areas).toEqual(["working", "completed", "pre-head", "head"]);
    const segments = [...table.querySelectorAll("tbody tr td:nth-child(2)")].map((td) => td.textContent);
    expect(segments).toEqual(["4", "212", "0", "848"]);
  });

  test("per-node tables are titled by node role, with columns in pipeline order", () => {
    const { container } = renderPanel(backlogFixture());
    const origins = tableUnder(container, "Origins (segmentation)");
    expect(headerTexts(origins)).toEqual(["Node", "Working", "Completed"]);
    const homes = tableUnder(container, "Homes (collection)");
    expect(headerTexts(homes)).toEqual(["Node", "Pre-head", "Head"]);
    const names = [...origins.querySelectorAll("tbody tr td:first-child")].map((td) => td.textContent);
    expect(names).toEqual(["node-1", "node-2"]);
    expect(within(origins).getByText("53 (585.9 KiB)")).toBeDefined();
  });

  test("the heading's help button opens the pipeline backlog section", () => {
    const { getByText, openHelp } = renderPanel(backlogFixture());
    const h3 = getByText("Pipeline backlog");
    const button = h3.querySelector<HTMLElement>("button[aria-label='Help']");
    expect(button).not.toBeNull();
    fireEvent.click(button!);
    expect(openHelp).toHaveBeenCalledWith(PIPELINE_BACKLOG_HELP_REF);
    expect(PIPELINE_BACKLOG_HELP_REF).toBe("inspector-vaults#pipeline-backlog");
  });
});

describe("PipelineBacklogView empty state", () => {
  test("with no backlog from the connected node the panel keeps its heading and names the source", () => {
    const { getByText, queryByText } = renderPanel(null);
    expect(getByText("Pipeline backlog")).toBeDefined();
    expect(getByText(PIPELINE_BACKLOG_EMPTY_TEXT)).toBeDefined();
    expect(PIPELINE_BACKLOG_EMPTY_TEXT).toContain("connected node");
    expect(queryByText("Eligible / registry segments")).toBeNull();
    expect(queryByText("Staging area")).toBeNull();
  });

  test("with a backlog the empty-state text is absent", () => {
    const { queryByText } = renderPanel(backlogFixture());
    expect(queryByText(PIPELINE_BACKLOG_EMPTY_TEXT)).toBeNull();
  });
});
