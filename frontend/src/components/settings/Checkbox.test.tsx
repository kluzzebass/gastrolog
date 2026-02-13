import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render, fireEvent } from "@testing-library/react";
import { Checkbox } from "./Checkbox";

describe("Checkbox", () => {
  test("renders unchecked (no checkmark SVG)", () => {
    const { container } = render(
      <Checkbox checked={false} onChange={() => {}} dark={true} />,
    );
    expect(container.querySelector("svg")).toBeNull();
  });

  test("renders checked with checkmark SVG", () => {
    const { container } = render(
      <Checkbox checked={true} onChange={() => {}} dark={true} />,
    );
    expect(container.querySelector("svg")).not.toBeNull();
  });

  test("calls onChange with toggled value on click", () => {
    const onChange = mock(() => {});
    const { container } = render(
      <Checkbox checked={false} onChange={onChange} dark={true} />,
    );
    fireEvent.click(container.querySelector(".cursor-pointer")!);
    expect(onChange).toHaveBeenCalledWith(true);
  });

  test("calls onChange with false when checked", () => {
    const onChange = mock(() => {});
    const { container } = render(
      <Checkbox checked={true} onChange={onChange} dark={true} />,
    );
    fireEvent.click(container.querySelector(".cursor-pointer")!);
    expect(onChange).toHaveBeenCalledWith(false);
  });

  test("renders label when provided", () => {
    const { getByText } = render(
      <Checkbox checked={false} onChange={() => {}} dark={true} label="Enable feature" />,
    );
    expect(getByText("Enable feature")).toBeTruthy();
  });

  test("does not render label when omitted", () => {
    const { queryByText } = render(
      <Checkbox checked={false} onChange={() => {}} dark={true} />,
    );
    expect(queryByText("Enable feature")).toBeNull();
  });

  test("checked state has copper styling", () => {
    const { container } = render(
      <Checkbox checked={true} onChange={() => {}} dark={true} />,
    );
    const btn = container.querySelector("button");
    expect(btn?.className).toContain("bg-copper");
  });
});
