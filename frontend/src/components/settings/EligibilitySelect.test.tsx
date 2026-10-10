import { describe, test, expect, mock } from "bun:test";
import { fireEvent } from "@testing-library/react";
import { HelpProvider } from "../../hooks/useHelp";
import { renderWritable } from "../../testing/renderWritable";
import { EligibilitySelect, type EligibilityOption } from "./EligibilitySelect";

function renderSelect(props: {
  value: string;
  options: EligibilityOption[];
  placeholder?: string;
  emptyMessage?: string;
}) {
  const openHelp = mock((_topicId?: string) => {});
  const onChange = mock((_v: string) => {});
  const result = renderWritable(
    <HelpProvider onOpen={openHelp}>
      <EligibilitySelect {...props} onChange={onChange} helpRef="topic#section" dark />
    </HelpProvider>,
  );
  return { ...result, openHelp, onChange };
}

describe("EligibilitySelect", () => {
  test("ineligible options are disabled and carry their reason", () => {
    const { container } = renderSelect({
      value: "",
      placeholder: "Pick one...",
      options: [
        { value: "a", label: "Alpha" },
        { value: "b", label: "Bravo", ineligibleReason: "too small" },
      ],
    });
    const options = [...container.querySelectorAll("option")].map((o) => [o.textContent, o.disabled]);
    expect(options).toEqual([
      ["Pick one...", false],
      ["Alpha", false],
      ["Bravo — too small", true],
    ]);
  });

  test("the empty message shows only when nothing is eligible, and links to the rule", () => {
    const empty = renderSelect({
      value: "",
      emptyMessage: "Nothing qualifies.",
      options: [{ value: "b", label: "Bravo", ineligibleReason: "too small" }],
    });
    expect(empty.getByRole("note").textContent).toContain("Nothing qualifies.");
    fireEvent.click(empty.getByText("Eligibility rules"));
    expect(empty.openHelp).toHaveBeenCalledWith("topic#section");
    empty.unmount();

    const some = renderSelect({
      value: "",
      emptyMessage: "Nothing qualifies.",
      options: [{ value: "a", label: "Alpha" }],
    });
    expect(some.queryByRole("note")).toBeNull();
  });

  test("a current value that does not qualify is named with its reason", () => {
    const { getByRole } = renderSelect({
      value: "b",
      options: [
        { value: "a", label: "Alpha" },
        { value: "b", label: "Bravo", ineligibleReason: "too small" },
      ],
    });
    expect(getByRole("note").textContent).toContain("Bravo is not eligible — too small.");
  });
});
