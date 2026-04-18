import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render, fireEvent } from "@testing-library/react";
import { Button, IconButton } from "./Buttons";

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

describe("IconButton", () => {
  test("renders + glyph for add intent", () => {
    const { getByText } = render(
      <IconButton intent="add" onClick={() => {}} title="Add row" />,
    );
    expect(getByText("+")).toBeTruthy();
  });

  test("renders × glyph for remove intent", () => {
    const { getByText } = render(
      <IconButton intent="remove" onClick={() => {}} title="Delete row" />,
    );
    expect(getByText("×")).toBeTruthy();
  });

  test("sets title attribute", () => {
    const { getByTitle } = render(
      <IconButton intent="add" onClick={() => {}} title="Add row" />,
    );
    expect(getByTitle("Add row")).toBeTruthy();
  });

  test("fires onClick", () => {
    const onClick = mock(() => {});
    const { getByTitle } = render(
      <IconButton intent="add" onClick={onClick} title="Add row" />,
    );
    fireEvent.click(getByTitle("Add row"));
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  test("respects disabled prop", () => {
    const onClick = mock(() => {});
    const { getByTitle } = render(
      <IconButton intent="add" onClick={onClick} title="Add row" disabled />,
    );
    const btn = getByTitle("Add row");
    expect(btn.getAttribute("disabled")).toBe("");
    fireEvent.click(btn);
    expect(onClick).not.toHaveBeenCalled();
  });

  test("applies cursor-pointer and shared sizing classes", () => {
    const { getByTitle } = render(
      <IconButton intent="add" onClick={() => {}} title="Add row" />,
    );
    const cls = getByTitle("Add row").className;
    expect(cls).toContain("cursor-pointer");
    expect(cls).toContain("p-1.5");
    expect(cls).toContain("text-base");
    expect(cls).toContain("leading-none");
  });

  test("forwards onPointerDown (used by drag rows to stop propagation)", () => {
    const onPointerDown = mock(() => {});
    const { getByTitle } = render(
      <IconButton intent="remove" onClick={() => {}} title="Delete row" onPointerDown={onPointerDown} />,
    );
    fireEvent.pointerDown(getByTitle("Delete row"));
    expect(onPointerDown).toHaveBeenCalledTimes(1);
  });
});
