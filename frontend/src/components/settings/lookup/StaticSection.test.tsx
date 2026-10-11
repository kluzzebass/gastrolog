import { describe, test, expect } from "bun:test";
import { fireEvent } from "@testing-library/react";
import { installMockClients } from "../../../../test/api-mock";
import { createTestQueryClient, settingsWrapper } from "../../../../test/render";

installMockClients();

import { StaticAddForm, StaticCards } from "./StaticSection";
import type { StaticLookupDraft } from "./types";
import { renderWritable } from "../../../testing/renderWritable";

function renderCards(lookups: StaticLookupDraft[]) {
  const qc = createTestQueryClient();
  return renderWritable(
    <StaticCards
      dark
      lookups={lookups}
      savedLookups={[]}
      onUpdate={() => {}}
      onDelete={() => {}}
      onRevert={() => {}}
    />,
    { wrapper: settingsWrapper(qc) },
  );
}

describe("StaticSection save gates", () => {
  test("an empty column header disables Save with the reason", () => {
    const { getByText } = renderCards([{ name: "teams", keyColumn: "", valueColumns: ["owner"], rows: [] }]);

    fireEvent.click(getByText("teams"));
    expect(getByText("Save").closest("button")!.disabled).toBe(true);
    expect(getByText("Every column needs a header")).toBeTruthy();
  });

  test("a duplicate key value disables Save with the reason", () => {
    const { getByText } = renderCards([
      { name: "teams", keyColumn: "team", valueColumns: ["owner"], rows: [{ team: "a", owner: "x" }, { team: "a", owner: "y" }] },
    ]);

    fireEvent.click(getByText("teams"));
    expect(getByText("Key column has empty or duplicate values")).toBeTruthy();
  });

  test("a complete lookup saves with nothing beside the button", () => {
    const { getByText, queryByText } = renderCards([
      { name: "teams", keyColumn: "team", valueColumns: ["owner"], rows: [{ team: "a", owner: "x" }] },
    ]);

    fireEvent.click(getByText("teams"));
    expect(getByText("Save").closest("button")!.disabled).toBe(false);
    expect(queryByText(/needs a header|duplicate values|is required/)).toBeNull();
  });
});

describe("StaticSection create gate", () => {
  test("a new lookup starts with the header reason beside Create", () => {
    const qc = createTestQueryClient();
    const { getByText } = renderWritable(
      <StaticAddForm
        dark
        addToast={() => {}}
        onCreated={() => {}}
        onCancel={() => {}}
        existingLookups={[]}
        namePlaceholder="fresh-lookup"
      />,
      { wrapper: settingsWrapper(qc) },
    );

    expect(getByText("Create").closest("button")!.disabled).toBe(true);
    expect(getByText("Every column needs a header")).toBeTruthy();
  });
});
