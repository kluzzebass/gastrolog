import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../test/api-mock";

// useValidation imports queryClient (the RPC client, not react-query).
const mocks = installMockClients();

import { useValidation } from "./useValidation";

beforeEach(() => {
  m(mocks.queryClient, "validateQuery").mockClear();
});

describe("useValidation", () => {
  test("empty expression is always valid", () => {
    const { result } = renderHook(() => useValidation(""));
    expect(result.current.valid).toBe(true);
    expect(result.current.errorMessage).toBeNull();
    expect(result.current.canFollow).toBe(true);
  });

  test("whitespace-only expression is valid", () => {
    const { result } = renderHook(() => useValidation("   "));
    expect(result.current.valid).toBe(true);
  });

  test("validates expression after debounce", async () => {
    m(mocks.queryClient, "validateQuery").mockResolvedValue({
      valid: true,
      errorMessage: "",
      errorOffset: -1,
      spans: [{ text: "error", role: "token" }],
      expression: "error",
      hasPipeline: false,
      canFollow: true,
    });

    const { result } = renderHook(() => useValidation("error"));

    await waitFor(() => expect(result.current.spans).toHaveLength(1));
    expect(result.current.valid).toBe(true);
    expect(result.current.spans[0]?.text).toBe("error");
  });

  test("reports invalid expression", async () => {
    m(mocks.queryClient, "validateQuery").mockResolvedValue({
      valid: false,
      errorMessage: "unexpected token",
      errorOffset: 5,
      spans: [],
      expression: "error |",
      hasPipeline: true,
      canFollow: false,
    });

    const { result } = renderHook(() => useValidation("error |"));

    await waitFor(() => expect(result.current.valid).toBe(false));
    expect(result.current.errorMessage).toBe("unexpected token");
    expect(result.current.errorOffset).toBe(5);
    expect(result.current.hasPipeline).toBe(true);
  });

  test("does not call backend for empty expression", () => {
    renderHook(() => useValidation(""));
    expect(m(mocks.queryClient, "validateQuery")).not.toHaveBeenCalled();
  });
});
