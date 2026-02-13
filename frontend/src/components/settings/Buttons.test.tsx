import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render, fireEvent } from "@testing-library/react";
import { PrimaryButton, GhostButton } from "./Buttons";

describe("PrimaryButton", () => {
  test("renders children", () => {
    const { getByText } = render(<PrimaryButton onClick={() => {}}>Save</PrimaryButton>);
    expect(getByText("Save")).toBeTruthy();
  });

  test("calls onClick when clicked", () => {
    const onClick = mock(() => {});
    const { getByText } = render(<PrimaryButton onClick={onClick}>Save</PrimaryButton>);
    fireEvent.click(getByText("Save"));
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  test("respects disabled prop", () => {
    const onClick = mock(() => {});
    const { getByText } = render(
      <PrimaryButton onClick={onClick} disabled>Save</PrimaryButton>,
    );
    const btn = getByText("Save");
    expect(btn.getAttribute("disabled")).toBe("");
    fireEvent.click(btn);
    expect(onClick).not.toHaveBeenCalled();
  });

  test("has copper background styling", () => {
    const { getByText } = render(<PrimaryButton onClick={() => {}}>Save</PrimaryButton>);
    expect(getByText("Save").className).toContain("bg-copper");
  });
});

describe("GhostButton", () => {
  test("renders children", () => {
    const { getByText } = render(
      <GhostButton onClick={() => {}} dark={true}>Cancel</GhostButton>,
    );
    expect(getByText("Cancel")).toBeTruthy();
  });

  test("calls onClick when clicked", () => {
    const onClick = mock(() => {});
    const { getByText } = render(
      <GhostButton onClick={onClick} dark={true}>Cancel</GhostButton>,
    );
    fireEvent.click(getByText("Cancel"));
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  test("bordered adds border class", () => {
    const { getByText } = render(
      <GhostButton onClick={() => {}} dark={true} bordered>Cancel</GhostButton>,
    );
    expect(getByText("Cancel").className).toContain("border");
  });

  test("applies extra className", () => {
    const { getByText } = render(
      <GhostButton onClick={() => {}} dark={true} className="ml-2">Cancel</GhostButton>,
    );
    expect(getByText("Cancel").className).toContain("ml-2");
  });
});
