import { describe, expect, test, mock } from "bun:test";
import React from "react";
import { render, fireEvent } from "@testing-library/react";
import { CloseButton, DialogTabHeader } from "./Dialog";

// Dialog itself uses focus-trap-react which is incompatible with happy-dom.
// Test CloseButton and DialogTabHeader directly.

describe("CloseButton", () => {
  test("renders with aria-label", () => {
    const { getByLabelText } = render(
      <CloseButton onClick={() => {}} dark={true} />,
    );
    expect(getByLabelText("Close")).toBeTruthy();
  });

  test("calls onClick when clicked", () => {
    const onClick = mock(() => {});
    const { getByLabelText } = render(
      <CloseButton onClick={onClick} dark={true} />,
    );
    fireEvent.click(getByLabelText("Close"));
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  test("renders × character", () => {
    const { getByLabelText } = render(
      <CloseButton onClick={() => {}} dark={true} />,
    );
    expect(getByLabelText("Close").textContent).toBe("×");
  });
});

describe("DialogTabHeader", () => {
  const tabs = [
    { id: "general", label: "General", icon: ({ className }: { className?: string }) => <span className={className}>G</span> },
    { id: "advanced", label: "Advanced", icon: ({ className }: { className?: string }) => <span className={className}>A</span> },
  ];

  test("renders title", () => {
    const { getByText } = render(
      <DialogTabHeader title="Settings" tabs={tabs} activeTab="general" onTabChange={() => {}} onClose={() => {}} dark={true} />,
    );
    expect(getByText("Settings")).toBeTruthy();
  });

  test("renders all tabs", () => {
    const { getByText } = render(
      <DialogTabHeader title="Settings" tabs={tabs} activeTab="general" onTabChange={() => {}} onClose={() => {}} dark={true} />,
    );
    expect(getByText("General")).toBeTruthy();
    expect(getByText("Advanced")).toBeTruthy();
  });

  test("active tab has copper styling", () => {
    const { getByText } = render(
      <DialogTabHeader title="Settings" tabs={tabs} activeTab="general" onTabChange={() => {}} onClose={() => {}} dark={true} />,
    );
    expect(getByText("General").closest("button")!.className).toContain("text-copper");
    expect(getByText("Advanced").closest("button")!.className).not.toContain("text-copper");
  });

  test("clicking a tab calls onTabChange", () => {
    const onTabChange = mock(() => {});
    const { getByText } = render(
      <DialogTabHeader title="Settings" tabs={tabs} activeTab="general" onTabChange={onTabChange} onClose={() => {}} dark={true} />,
    );
    fireEvent.click(getByText("Advanced"));
    expect(onTabChange).toHaveBeenCalledWith("advanced");
  });

  test("close button calls onClose", () => {
    const onClose = mock(() => {});
    const { getByLabelText } = render(
      <DialogTabHeader title="Settings" tabs={tabs} activeTab="general" onTabChange={() => {}} onClose={onClose} dark={true} />,
    );
    fireEvent.click(getByLabelText("Close"));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  test("tab icons are rendered", () => {
    const { getByText } = render(
      <DialogTabHeader title="Settings" tabs={tabs} activeTab="general" onTabChange={() => {}} onClose={() => {}} dark={true} />,
    );
    expect(getByText("G")).toBeTruthy();
    expect(getByText("A")).toBeTruthy();
  });
});
