import { describe, test, expect, mock } from "bun:test";
import { fireEvent, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../../test/render";
import { encode } from "../../../api/glid";
import { HelpProvider } from "../../../hooks/useHelp";

const mocks = installMockClients();

import { CsvAddForm, CsvCards } from "./CsvSection";
import { fileLookupIncompleteReason, type LookupSectionProps } from "./types";
import { renderWritable } from "../../../testing/renderWritable";

function testId(n: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(16);
  bytes[15] = n;
  return bytes;
}

const assetsFile = { id: testId(1), name: "assets.csv", size: 0n, sha256: "", uploadedAt: "" };

function renderCards() {
  m(mocks.systemClient, "previewCSVLookup").mockResolvedValue({
    columns: ["ip", "host"],
    rows: [],
    totalRows: 0,
    error: "",
  });
  const qc = createTestQueryClient();
  const openHelp = mock((_helpRef?: string) => {});
  const sectionProps = {
    dark: true,
    managedFiles: [assetsFile],
    uploadFile: { isPending: false, mutate: () => {} },
    addToast: () => {},
  } as unknown as LookupSectionProps;
  const result = renderWritable(
    <HelpProvider onOpen={openHelp}>
      <CsvCards
        {...sectionProps}
        lookups={[{ name: "assets", fileId: encode(testId(1)), keyColumn: "", valueColumns: [] }]}
        savedLookups={[]}
        onUpdate={() => {}}
        onDelete={() => {}}
        onRevert={() => {}}
      />
    </HelpProvider>,
    { wrapper: settingsWrapper(qc) },
  );
  return { ...result, openHelp };
}

/** The help button beside a form field's label. */
function fieldHelpButton(label: HTMLElement): HTMLButtonElement {
  const button = label.parentElement!.querySelector('button[aria-label="Help"]');
  expect(button).not.toBeNull();
  return button as HTMLButtonElement;
}

describe("CsvCards labels", () => {
  test("the name field says it is the table name used in queries", async () => {
    const { getByText } = renderCards();
    fireEvent.click(getByText("assets"));
    await waitFor(() => expect(getByText("Table name used in queries, e.g. | lookup assets ip")).toBeTruthy());
  });
});

describe("CsvCards help", () => {
  test("the column pickers open the CSV lookups section", async () => {
    const { getByText, openHelp } = renderCards();
    fireEvent.click(getByText("assets"));

    await waitFor(() => expect(getByText("Key Column")).toBeTruthy());

    fireEvent.click(fieldHelpButton(getByText("Key Column")));
    expect(openHelp).toHaveBeenCalledWith("lookups-settings#csv-file-lookups");

    fireEvent.click(fieldHelpButton(getByText("Value Columns")));
    expect(openHelp).toHaveBeenLastCalledWith("lookups-settings#csv-file-lookups");
  });
});

// A file-backed lookup without a file is dropped on save, so Create waits for
// one and says so; picking an existing file clears the reason.
describe("CsvAddForm create gate", () => {
  test("Create is disabled with 'Select a file first' until a file is picked", async () => {
    m(mocks.systemClient, "previewCSVLookup").mockResolvedValue({ columns: ["ip"], rows: [], totalRows: 0, error: "" });
    const qc = createTestQueryClient();
    const sectionProps = {
      dark: true,
      managedFiles: [assetsFile],
      uploadFile: { isPending: false, mutate: () => {} },
      addToast: () => {},
    } as unknown as LookupSectionProps;
    const { getByText, queryByText } = renderWritable(
      <HelpProvider onOpen={() => {}}>
        <CsvAddForm {...sectionProps} onCreated={() => {}} onCancel={() => {}} existingLookups={[]} namePlaceholder="calm-otter" />
      </HelpProvider>,
      { wrapper: settingsWrapper(qc) },
    );
    const create = getByText("Create").closest("button")!;
    expect(create.disabled).toBe(true);
    expect(getByText("Select a file first")).toBeTruthy();

    fireEvent.click(getByText("assets.csv").closest("button")!);
    await waitFor(() => expect(getByText("Create").closest("button")!.disabled).toBe(false));
    expect(queryByText("Select a file first")).toBeNull();
  });
});

describe("fileLookupIncompleteReason", () => {
  test("names the missing step: a name, then a file", () => {
    expect(fileLookupIncompleteReason("", undefined, "")).toBe("Name is required");
    expect(fileLookupIncompleteReason("", "calm-otter", "")).toBe("Select a file first");
    expect(fileLookupIncompleteReason("assets", undefined, "f1")).toBeUndefined();
  });
});
