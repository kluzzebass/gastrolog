import { describe, test, expect, mock, beforeEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { renderHookWithAbandonedRender } from "../../../test/abandonedRender";

const mocks = installMockClients();

import { useExplain } from "./useExplain";

const explainMock = () => m(mocks.queryClient, "explain");

beforeEach(() => {
  explainMock().mockReset();
});

describe("useExplain", () => {
  test("a successful explain fills the plan", async () => {
    explainMock().mockResolvedValueOnce({
      chunks: [],
      direction: "reverse",
      totalChunks: 7,
      expression: "level=error",
      pipelineStages: [],
    });
    const { result } = renderHook(() => useExplain());

    await act(async () => {
      await result.current.explain("level=error");
    });

    expect(result.current.direction).toBe("reverse");
    expect(result.current.totalChunks).toBe(7);
    expect(result.current.isLoading).toBe(false);
    expect(result.current.error).toBeNull();
  });

  test("a failed explain reports the error to the latest onError", async () => {
    const boom = new Error("boom");
    explainMock().mockRejectedValueOnce(boom);
    const first = mock((_err: Error) => {});
    const latest = mock((_err: Error) => {});
    const { result, rerender } = renderHook(({ onError }) => useExplain({ onError }), {
      initialProps: { onError: first },
    });
    rerender({ onError: latest });

    await act(async () => {
      await result.current.explain("x");
    });

    expect(result.current.error).toBe(boom);
    expect(latest).toHaveBeenCalledWith(boom);
    expect(first).not.toHaveBeenCalled();
  });

  test("an error never reaches the onError of a render React threw away", async () => {
    explainMock().mockRejectedValueOnce(new Error("boom"));
    const committed = mock((_err: Error) => {});
    const abandoned = mock((_err: Error) => {});
    const { result, abandon } = renderHookWithAbandonedRender(
      (onError: (err: Error) => void) => useExplain({ onError }),
      committed,
    );

    await abandon(abandoned);
    await act(async () => {
      await result.current.explain("x");
    });

    expect(abandoned).not.toHaveBeenCalled();
    expect(committed).toHaveBeenCalledTimes(1);
  });
});
