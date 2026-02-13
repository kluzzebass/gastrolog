import { describe, expect, test } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { usePanelResize } from "./usePanelResize";

describe("usePanelResize", () => {
  test("returns handleResize and resizing=false initially", () => {
    let width = 300;
    const { result } = renderHook(() =>
      usePanelResize((w) => (width = w), 100, 500, "right"),
    );

    expect(result.current.resizing).toBe(false);
    expect(typeof result.current.handleResize).toBe("function");
  });

  test("handleResize sets resizing to true", () => {
    let width = 300;
    const { result } = renderHook(() =>
      usePanelResize((w) => (width = w), 100, 500, "right"),
    );

    act(() => {
      // Simulate mousedown event
      const fakeEvent = {
        preventDefault: () => {},
      } as React.MouseEvent;
      result.current.handleResize(fakeEvent);
    });

    expect(result.current.resizing).toBe(true);
  });

  test("mouseup after resize sets resizing back to false", () => {
    let width = 300;
    const { result } = renderHook(() =>
      usePanelResize((w) => (width = w), 100, 500, "right"),
    );

    act(() => {
      const fakeEvent = {
        preventDefault: () => {},
      } as React.MouseEvent;
      result.current.handleResize(fakeEvent);
    });
    expect(result.current.resizing).toBe(true);

    act(() => {
      window.dispatchEvent(new MouseEvent("mouseup"));
    });
    expect(result.current.resizing).toBe(false);
  });

  test("mousemove calls setter with clamped value (right direction)", () => {
    let width = 300;
    const { result } = renderHook(() =>
      usePanelResize((w) => (width = w), 100, 500, "right"),
    );

    // Override innerWidth for predictable calculation
    Object.defineProperty(window, "innerWidth", { value: 1000, writable: true });

    act(() => {
      const fakeEvent = {
        preventDefault: () => {},
      } as React.MouseEvent;
      result.current.handleResize(fakeEvent);
    });

    // Simulate mouse at x=700 → right direction = 1000 - 700 = 300
    act(() => {
      window.dispatchEvent(new MouseEvent("mousemove", { clientX: 700 }));
    });
    expect(width).toBe(300);

    // Mouse at x=950 → right = 1000 - 950 = 50, clamped to min=100
    act(() => {
      window.dispatchEvent(new MouseEvent("mousemove", { clientX: 950 }));
    });
    expect(width).toBe(100);

    // Mouse at x=100 → right = 1000 - 100 = 900, clamped to max=500
    act(() => {
      window.dispatchEvent(new MouseEvent("mousemove", { clientX: 100 }));
    });
    expect(width).toBe(500);

    // Clean up
    act(() => {
      window.dispatchEvent(new MouseEvent("mouseup"));
    });
  });

  test("mousemove with left direction uses clientX directly", () => {
    let width = 300;
    const { result } = renderHook(() =>
      usePanelResize((w) => (width = w), 100, 500, "left"),
    );

    act(() => {
      const fakeEvent = {
        preventDefault: () => {},
      } as React.MouseEvent;
      result.current.handleResize(fakeEvent);
    });

    // Mouse at x=250 → left direction = 250
    act(() => {
      window.dispatchEvent(new MouseEvent("mousemove", { clientX: 250 }));
    });
    expect(width).toBe(250);

    // Mouse at x=50 → clamped to min=100
    act(() => {
      window.dispatchEvent(new MouseEvent("mousemove", { clientX: 50 }));
    });
    expect(width).toBe(100);

    // Mouse at x=600 → clamped to max=500
    act(() => {
      window.dispatchEvent(new MouseEvent("mousemove", { clientX: 600 }));
    });
    expect(width).toBe(500);

    act(() => {
      window.dispatchEvent(new MouseEvent("mouseup"));
    });
  });
});
