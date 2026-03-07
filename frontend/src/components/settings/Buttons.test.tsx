import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render, fireEvent } from "@testing-library/react";
import { Button } from "./Buttons";

describe("PrimaryButton", () => {
  test("renders children", () => {
    const { getByText } = render(<Button onClick={() => {}}>Save</Button>);
    expect(getByText("Save")).toBeTruthy();
  });

  test("calls onClick when clicked", () => {
    const onClick = mock(() => {});
    const { getByText } = render(<Button onClick={onClick}>Save</Button>);
    fireEvent.click(getByText("Save"));
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  test("respects disabled prop", () => {
    const onClick = mock(() => {});
    const { getByText } = render(
      <Button onClick={onClick} disabled>Save</Button>,
    );
    const btn = getByText("Save");
    expect(btn.getAttribute("disabled")).toBe("");
    fireEvent.click(btn);
    expect(onClick).not.toHaveBeenCalled();
  });

  test("renders as a button element", () => {
    const { getByText } = render(<Button onClick={() => {}}>Save</Button>);
    expect(getByText("Save").tagName).toBe("BUTTON");
  });
});

describe("GhostButton", () => {
  test("renders children", () => {
    const { getByText } = render(
      <Button variant="ghost" onClick={() => {}} dark={true}>Cancel</Button>,
    );
    expect(getByText("Cancel")).toBeTruthy();
  });

  test("calls onClick when clicked", () => {
    const onClick = mock(() => {});
    const { getByText } = render(
      <Button variant="ghost" onClick={onClick} dark={true}>Cancel</Button>,
    );
    fireEvent.click(getByText("Cancel"));
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  test("bordered variant differs from non-bordered", () => {
    const { getByText: g1 } = render(
      <Button variant="ghost" onClick={() => {}} dark={true} bordered>Bordered</Button>,
    );
    const { getByText: g2 } = render(
      <Button variant="ghost" onClick={() => {}} dark={true}>Plain</Button>,
    );
    // Bordered and non-bordered should produce different class lists
    expect(g1("Bordered").className).not.toBe(g2("Plain").className);
  });

  test("applies extra className", () => {
    const { getByText } = render(
      <Button variant="ghost" onClick={() => {}} dark={true} className="ml-2">Cancel</Button>,
    );
    expect(getByText("Cancel").className).toContain("ml-2");
  });
});
