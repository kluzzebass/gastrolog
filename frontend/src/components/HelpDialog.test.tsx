import { describe, test, expect, beforeEach, afterEach, spyOn } from "bun:test";
import { render, waitFor, fireEvent } from "@testing-library/react";
import { HelpDialog } from "./HelpDialog";

let scrolled: Element[] = [];
let spy: ReturnType<typeof spyOn<Element, "scrollIntoView">>;

beforeEach(() => {
  scrolled = [];
  spy = spyOn(Element.prototype, "scrollIntoView").mockImplementation(function (this: Element) {
    scrolled.push(this);
  });
});

afterEach(() => {
  spy.mockRestore();
});

function anchorOf(el: Element): string | undefined {
  return (el as HTMLElement).dataset.helpAnchor;
}

describe("HelpDialog section anchors", () => {
  test("a reference with an anchor scrolls that section into view once its topic renders", async () => {
    render(
      <HelpDialog dark topicId="vaults-config#transfer-target" onClose={() => {}} onNavigate={() => {}} />,
    );

    await waitFor(() => expect(scrolled.map(anchorOf)).toEqual(["transfer-target"]));
    expect(scrolled[0]!.textContent).toBe("Transfer Target");
  });

  test("a reference without an anchor scrolls no section", async () => {
    const { findByText } = render(
      <HelpDialog dark topicId="vaults-config" onClose={() => {}} onNavigate={() => {}} />,
    );

    await findByText("Transfer Target", { selector: "h3" });
    expect(scrolled).toEqual([]);
  });

  test("an in-topic link carries its anchor to navigation", async () => {
    const navigated: string[] = [];
    const { findAllByText } = render(
      <HelpDialog dark topicId="vaults-config" onClose={() => {}} onNavigate={(id) => navigated.push(id)} />,
    );

    const [link] = await findAllByText("Transfer Target", { selector: "button" });
    fireEvent.click(link!);
    expect(navigated).toEqual(["vaults-config#transfer-target"]);
  });
});
