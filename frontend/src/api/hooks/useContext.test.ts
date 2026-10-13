import { describe, test, expect, mock, beforeEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { renderHookWithAbandonedRender } from "../../../test/abandonedRender";

const mocks = installMockClients();

import { RecordRef } from "../client";
import { useRecordContext } from "./useContext";

const getContextMock = () => m(mocks.queryClient, "getContext");
const ref = new RecordRef({ chunkId: new Uint8Array([1]), pos: BigInt(3), vaultId: new Uint8Array([2]) });

beforeEach(() => {
  getContextMock().mockReset();
});

describe("useRecordContext", () => {
  test("fetchContext fills before/anchor/after", async () => {
    const anchor = { raw: new Uint8Array([1]) };
    getContextMock().mockResolvedValueOnce({ before: [{}, {}], anchor, after: [{}] });
    const { result } = renderHook(() => useRecordContext());

    await act(async () => {
      await result.current.fetchContext(ref);
    });

    expect(result.current.before).toHaveLength(2);
    expect(result.current.anchor).toBe(anchor as never);
    expect(result.current.after).toHaveLength(1);
    expect(result.current.isLoading).toBe(false);
  });

  test("a failed fetch reports the error to the latest onError", async () => {
    const boom = new Error("boom");
    getContextMock().mockRejectedValueOnce(boom);
    const first = mock((_err: Error) => {});
    const latest = mock((_err: Error) => {});
    const { result, rerender } = renderHook(({ onError }) => useRecordContext({ onError }), {
      initialProps: { onError: first },
    });
    rerender({ onError: latest });

    await act(async () => {
      await result.current.fetchContext(ref);
    });

    expect(result.current.error).toBe(boom);
    expect(latest).toHaveBeenCalledWith(boom);
    expect(first).not.toHaveBeenCalled();
  });

  test("an error never reaches the onError of a render React threw away", async () => {
    getContextMock().mockRejectedValueOnce(new Error("boom"));
    const committed = mock((_err: Error) => {});
    const abandoned = mock((_err: Error) => {});
    const { result, abandon } = renderHookWithAbandonedRender(
      (onError: (err: Error) => void) => useRecordContext({ onError }),
      committed,
    );

    await abandon(abandoned);
    await act(async () => {
      await result.current.fetchContext(ref);
    });

    expect(abandoned).not.toHaveBeenCalled();
    expect(committed).toHaveBeenCalledTimes(1);
  });
});
