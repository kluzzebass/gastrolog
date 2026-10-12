import { describe, test, expect, mock, beforeEach } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { renderHookWithAbandonedRender } from "../../../test/abandonedRender";

const mocks = installMockClients();

import { extractTokens, useSearch } from "./useSearch";

describe("extractTokens", () => {
  test("extracts bare words", () => {
    expect(extractTokens("error timeout")).toEqual(["error", "timeout"]);
  });

  test("lowercases tokens", () => {
    expect(extractTokens("ERROR Timeout")).toEqual(["error", "timeout"]);
  });

  test("filters out AND operator", () => {
    expect(extractTokens("error AND timeout")).toEqual(["error", "timeout"]);
  });

  test("filters out OR operator", () => {
    expect(extractTokens("error OR warning")).toEqual(["error", "warning"]);
  });

  test("filters out NOT operator", () => {
    expect(extractTokens("NOT debug")).toEqual(["debug"]);
  });

  test("operators are case-insensitive", () => {
    expect(extractTokens("error and timeout")).toEqual(["error", "timeout"]);
    expect(extractTokens("error or timeout")).toEqual(["error", "timeout"]);
    expect(extractTokens("not debug")).toEqual(["debug"]);
  });

  test("filters out reverse= directive", () => {
    expect(extractTokens("reverse=true error")).toEqual(["error"]);
  });

  test("filters out start= directive", () => {
    expect(extractTokens("start=2024-01-01 error")).toEqual(["error"]);
  });

  test("filters out end= directive", () => {
    expect(extractTokens("end=2024-12-31 error")).toEqual(["error"]);
  });

  test("filters out last= directive", () => {
    expect(extractTokens("last=5m error")).toEqual(["error"]);
  });

  test("filters out vault_id= directive", () => {
    expect(extractTokens("vault_id=myvault error")).toEqual(["error"]);
  });

  test("filters out limit= directive", () => {
    expect(extractTokens("limit=100 error")).toEqual(["error"]);
  });

  test("extracts values from key=value pairs", () => {
    expect(extractTokens("level=error")).toEqual(["error"]);
    expect(extractTokens("host=web-01 status=500")).toEqual(["web-01", "500"]);
  });

  test("excludes wildcard values (key=*)", () => {
    expect(extractTokens("level=*")).toEqual([]);
  });

  test("handles empty string", () => {
    expect(extractTokens("")).toEqual([]);
  });

  test("handles whitespace-only string", () => {
    expect(extractTokens("   ")).toEqual([]);
  });

  test("strips parentheses", () => {
    expect(extractTokens("(error OR warning)")).toEqual(["error", "warning"]);
  });

  test("handles multiple spaces between tokens", () => {
    expect(extractTokens("error   timeout")).toEqual(["error", "timeout"]);
  });

  test("handles mixed bare words and kv pairs", () => {
    expect(extractTokens("last=1h level=error timeout")).toEqual([
      "error",
      "timeout",
    ]);
  });
});

type Page = { records: { raw: Uint8Array }[]; resumeToken: Uint8Array; hasMore: boolean };

const searchMock = () => m(mocks.queryClient, "search");

function page(n: number, token: number | null): Page {
  return {
    records: Array.from({ length: n }, (_, i) => ({ raw: new Uint8Array([token ?? 0, i]) })),
    resumeToken: token === null ? new Uint8Array(0) : new Uint8Array([token]),
    hasMore: token !== null,
  };
}

/** Each search call streams the next page as a single response. */
function servePages(...pages: Page[]) {
  const requests: { resumeToken: Uint8Array }[] = [];
  searchMock().mockImplementation((req: { resumeToken: Uint8Array }) => {
    requests.push(req);
    const next = pages.shift();
    const responses = next ? [{ ...next, histogram: [], serverElapsedMs: BigInt(0) }] : [];
    return responses[Symbol.iterator]();
  });
  return requests;
}

function failSearch(err: Error) {
  searchMock().mockImplementation(() => ({
    [Symbol.asyncIterator]: () => ({ next: () => Promise.reject(err) }),
  }));
}

beforeEach(() => {
  searchMock().mockReset();
});

describe("useSearch", () => {
  test("loadMore appends the next page using the committed resume token", async () => {
    const requests = servePages(page(2, 7), page(1, null));
    const { result } = renderHook(() => useSearch());

    await act(async () => {
      await result.current.search("error");
    });
    expect(result.current.records).toHaveLength(2);
    expect(result.current.hasMore).toBe(true);

    await act(async () => {
      result.current.loadMore("error");
      await Promise.resolve();
    });

    expect(requests).toHaveLength(2);
    expect(requests[1]!.resumeToken).toEqual(new Uint8Array([7]));
    expect(result.current.records).toHaveLength(3);
    expect(result.current.hasMore).toBe(false);
  });

  test("loadMore is a no-op when the committed state has no more pages", async () => {
    const requests = servePages(page(2, null));
    const { result } = renderHook(() => useSearch());

    await act(async () => {
      await result.current.search("error");
    });
    act(() => result.current.loadMore("error"));

    expect(requests).toHaveLength(1);
  });

  test("loadMore reads committed state, not the state of a render React threw away", async () => {
    const requests = servePages(page(2, 7), page(1, null));
    const { result, abandon } = renderHookWithAbandonedRender(() => useSearch());

    await act(async () => {
      await result.current.search("error");
    });
    // A reset that renders but never commits: the committed results still have more pages.
    const committed = result.current;
    await abandon(undefined, () => committed.reset());

    await act(async () => {
      committed.loadMore("error");
      await Promise.resolve();
    });

    expect(requests).toHaveLength(2);
    expect(requests[1]!.resumeToken).toEqual(new Uint8Array([7]));
  });

  test("a failed search reports the error to the latest onError", async () => {
    const boom = new Error("boom");
    failSearch(boom);
    const first = mock((_err: Error) => {});
    const latest = mock((_err: Error) => {});
    const { result, rerender } = renderHook(({ onError }) => useSearch({ onError }), {
      initialProps: { onError: first },
    });
    rerender({ onError: latest });

    await act(async () => {
      await result.current.search("x");
    });

    expect(result.current.error).toBe(boom);
    expect(latest).toHaveBeenCalledWith(boom);
    expect(first).not.toHaveBeenCalled();
  });

  test("an error never reaches the onError of a render React threw away", async () => {
    failSearch(new Error("boom"));
    const committed = mock((_err: Error) => {});
    const abandoned = mock((_err: Error) => {});
    const { result, abandon } = renderHookWithAbandonedRender(
      (onError: (err: Error) => void) => useSearch({ onError }),
      committed,
    );

    await abandon(abandoned);
    await act(async () => {
      await result.current.search("x");
    });

    expect(abandoned).not.toHaveBeenCalled();
    expect(committed).toHaveBeenCalledTimes(1);
  });
});
