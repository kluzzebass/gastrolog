import { describe, test, expect, mock } from "bun:test";
import { renderHook } from "@testing-library/react";
import type { ReactNode } from "react";

import { HelpProvider, useHelp } from "./useHelp";

describe("useHelp", () => {
  test("throws when used outside HelpProvider", () => {
    // Suppress the expected React error boundary console noise.
    const spy = mock(() => {});
    const orig = console.error;
    console.error = spy;
    try {
      expect(() => renderHook(() => useHelp())).toThrow(
        "useHelp must be used within HelpProvider",
      );
    } finally {
      console.error = orig;
    }
  });

  test("returns openHelp from provider", () => {
    const onOpen = mock(() => {});
    const w = ({ children }: { children: ReactNode }) => (
      <HelpProvider onOpen={onOpen}>{children}</HelpProvider>
    );

    const { result } = renderHook(() => useHelp(), { wrapper: w });
    result.current.openHelp("some-topic");

    expect(onOpen).toHaveBeenCalledWith("some-topic");
  });

  test("openHelp works with no topic", () => {
    const onOpen = mock(() => {});
    const w = ({ children }: { children: ReactNode }) => (
      <HelpProvider onOpen={onOpen}>{children}</HelpProvider>
    );

    const { result } = renderHook(() => useHelp(), { wrapper: w });
    result.current.openHelp();

    expect(onOpen).toHaveBeenCalledTimes(1);
  });
});
