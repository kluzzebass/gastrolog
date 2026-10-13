import { describe, test, expect } from "bun:test";
import { useState } from "react";
import { render, within, fireEvent } from "@testing-library/react";
import { renderWithAbandonedRender } from "../../../test/abandonedRender";
import { renderWritable } from "../../testing/renderWritable";
import { EditableGrid } from "./EditableGrid";

type Row = Record<string, string>;

const columns = ["key", "value"];
const rowsOf = (...keys: string[]): Row[] => keys.map((k) => ({ key: k, value: `${k}-value` }));

function ControlledGrid({ initialRows }: Readonly<{ initialRows: Row[] }>) {
  const [rows, setRows] = useState(initialRows);
  return <EditableGrid dark columns={columns} rows={rows} onColumnsChange={() => {}} onRowsChange={setRows} />;
}

const page = () => within(document.body);
const cell = (value: string) => page().getByDisplayValue(value);

describe("EditableGrid row identity", () => {
  test("deleting a row keeps the inputs of the rows after it mounted", () => {
    renderWritable(<ControlledGrid initialRows={rowsOf("a", "b", "c")} />);
    const bInput = cell("b");
    const cInput = cell("c");

    fireEvent.click(page().getAllByTitle("Delete row")[0]!);

    expect(page().queryByDisplayValue("a")).toBeNull();
    expect(cell("b")).toBe(bInput);
    expect(cell("c")).toBe(cInput);
  });

  test("rows added by the parent get inputs without remounting existing rows", () => {
    const { rerender } = render(<EditableGrid dark columns={columns} rows={rowsOf("a", "b")} />);
    const aInput = cell("a");

    rerender(<EditableGrid dark columns={columns} rows={rowsOf("a", "b", "c")} />);

    expect(cell("a")).toBe(aInput);
    expect(cell("c")).toBeDefined();
  });

  test("a render React threw away does not remount committed rows", async () => {
    const { rerender, abandon } = renderWithAbandonedRender(
      (rows: Row[]) => <EditableGrid dark columns={columns} rows={rows} />,
      rowsOf("a", "b", "c"),
    );
    const cInput = cell("c");

    await abandon(rowsOf("a", "b"));
    rerender(rowsOf("a", "b", "c"));

    expect(cell("c")).toBe(cInput);
  });
});
