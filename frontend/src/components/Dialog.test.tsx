import { describe, expect, test, mock } from "bun:test";
import { render, fireEvent } from "@testing-library/react";
import { CloseButton } from "./Dialog";

// Dialog itself uses focus-trap-react which is incompatible with happy-dom.
// Test CloseButton directly.

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
