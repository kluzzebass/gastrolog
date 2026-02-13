import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render, fireEvent } from "@testing-library/react";
import { ExpandableCard } from "./ExpandableCard";

describe("ExpandableCard", () => {
  test("renders id label", () => {
    const { getByText } = render(
      <ExpandableCard id="my-store" dark={true}><p>content</p></ExpandableCard>,
    );
    expect(getByText("my-store")).toBeTruthy();
  });

  test("renders type badge when provided", () => {
    const { getByText } = render(
      <ExpandableCard id="my-store" typeBadge="file" dark={true}><p>content</p></ExpandableCard>,
    );
    expect(getByText("file")).toBeTruthy();
  });

  test("does not render type badge when omitted", () => {
    const { queryByText } = render(
      <ExpandableCard id="my-store" dark={true}><p>content</p></ExpandableCard>,
    );
    expect(queryByText("file")).toBeNull();
  });

  test("hides children when collapsed", () => {
    const { queryByText } = render(
      <ExpandableCard id="my-store" dark={true} expanded={false}><p>hidden content</p></ExpandableCard>,
    );
    expect(queryByText("hidden content")).toBeNull();
  });

  test("shows children when expanded", () => {
    const { getByText } = render(
      <ExpandableCard id="my-store" dark={true} expanded={true}><p>visible content</p></ExpandableCard>,
    );
    expect(getByText("visible content")).toBeTruthy();
  });

  test("calls onToggle when header is clicked", () => {
    const onToggle = mock(() => {});
    const { getByText } = render(
      <ExpandableCard id="my-store" dark={true} onToggle={onToggle}><p>content</p></ExpandableCard>,
    );
    fireEvent.click(getByText("my-store"));
    expect(onToggle).toHaveBeenCalledTimes(1);
  });

  test("has aria-expanded attribute", () => {
    const { container } = render(
      <ExpandableCard id="my-store" dark={true} expanded={true}><p>content</p></ExpandableCard>,
    );
    const header = container.querySelector("[aria-expanded]")!;
    expect(header.getAttribute("aria-expanded")).toBe("true");
  });

  test("renders status when provided", () => {
    const { getByText } = render(
      <ExpandableCard id="my-store" dark={true} status={<span>no policy</span>}>
        <p>content</p>
      </ExpandableCard>,
    );
    expect(getByText("no policy")).toBeTruthy();
  });

  test("renders headerRight when provided", () => {
    const { getByText } = render(
      <ExpandableCard id="my-store" dark={true} headerRight={<button>Edit</button>}>
        <p>content</p>
      </ExpandableCard>,
    );
    expect(getByText("Edit")).toBeTruthy();
  });

  test("accent badge has copper styling", () => {
    const { getByText } = render(
      <ExpandableCard id="my-store" typeBadge="active" typeBadgeAccent dark={true}>
        <p>content</p>
      </ExpandableCard>,
    );
    expect(getByText("active").className).toContain("text-copper");
  });
});
