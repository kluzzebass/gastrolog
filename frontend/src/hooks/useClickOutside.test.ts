import { describe, expect, test, mock } from "bun:test";
import { useRef } from "react";
import { renderHook, act } from "@testing-library/react";
import { useClickOutside } from "./useClickOutside";

describe("useClickOutside", () => {
  test("calls onClose when clicking outside the ref", () => {
    const onClose = mock(() => {});
    const outside = document.createElement("div");
    document.body.appendChild(outside);

    const inside = document.createElement("div");
    document.body.appendChild(inside);

    renderHook(() => {
      const ref = useRef<HTMLElement>(inside);
      useClickOutside(ref, onClose);
    });

    // Click outside
    act(() => {
      outside.dispatchEvent(new MouseEvent("mousedown", { bubbles: true }));
    });

    expect(onClose).toHaveBeenCalled();

    // Clean up
    document.body.removeChild(outside);
    document.body.removeChild(inside);
  });

  test("does not call onClose when clicking inside the ref", () => {
    const onClose = mock(() => {});
    const inside = document.createElement("div");
    const child = document.createElement("span");
    inside.appendChild(child);
    document.body.appendChild(inside);

    renderHook(() => {
      const ref = useRef<HTMLElement>(inside);
      useClickOutside(ref, onClose);
    });

    // Click inside (on child)
    act(() => {
      child.dispatchEvent(new MouseEvent("mousedown", { bubbles: true }));
    });

    expect(onClose).not.toHaveBeenCalled();

    // Clean up
    document.body.removeChild(inside);
  });

  test("calls onClose on Escape key", () => {
    const onClose = mock(() => {});
    const el = document.createElement("div");
    document.body.appendChild(el);

    renderHook(() => {
      const ref = useRef<HTMLElement>(el);
      useClickOutside(ref, onClose);
    });

    act(() => {
      window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    });

    expect(onClose).toHaveBeenCalled();

    // Clean up
    document.body.removeChild(el);
  });

  test("does not call onClose on non-Escape keys", () => {
    const onClose = mock(() => {});
    const el = document.createElement("div");
    document.body.appendChild(el);

    renderHook(() => {
      const ref = useRef<HTMLElement>(el);
      useClickOutside(ref, onClose);
    });

    act(() => {
      window.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter" }));
    });

    expect(onClose).not.toHaveBeenCalled();

    document.body.removeChild(el);
  });
});
