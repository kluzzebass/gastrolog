import { describe, test, expect } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { installMockClients } from "../../test/api-mock";
import { wrapper } from "../../test/render";

// useDialogState uses useIsFetching from react-query, so it needs a QueryClient.
installMockClients();

import { useDialogState } from "./useDialogState";

describe("useDialogState", () => {
  test("all dialogs start closed", () => {
    const { result } = renderHook(() => useDialogState(), { wrapper: wrapper() });
    expect(result.current.showPlan).toBe(false);
    expect(result.current.showHistory).toBe(false);
    expect(result.current.showSavedQueries).toBe(false);
    expect(result.current.showChangePassword).toBe(false);
    expect(result.current.showPreferences).toBe(false);
  });

  test("setters toggle individual dialogs", () => {
    const { result } = renderHook(() => useDialogState(), { wrapper: wrapper() });

    act(() => result.current.setShowPlan(true));
    expect(result.current.showPlan).toBe(true);

    act(() => result.current.setShowHistory(true));
    expect(result.current.showHistory).toBe(true);
    // Plan is still open — dialogs are independent.
    expect(result.current.showPlan).toBe(true);
  });

  test("inspectorGlow starts false", () => {
    const { result } = renderHook(() => useDialogState(), { wrapper: wrapper() });
    expect(result.current.inspectorGlow).toBe(false);
  });
});
