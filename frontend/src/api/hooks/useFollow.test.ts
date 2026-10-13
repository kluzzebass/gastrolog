import { describe, test, expect, mock, beforeEach } from "bun:test";
import { renderHook, act, waitFor } from "@testing-library/react";
import { Code, ConnectError } from "@connectrpc/connect";
import { installMockClients, m } from "../../../test/api-mock";
import { renderHookWithAbandonedRender } from "../../../test/abandonedRender";

const mocks = installMockClients();

import { useFollow } from "./useFollow";

type FollowResponse = { records: { raw: Uint8Array }[] };

/** A follow stream the test feeds by hand. */
function serveFollowStream() {
  const queue: FollowResponse[] = [];
  let failure: Error | null = null;
  let wake: (() => void) | null = null;
  const notify = () => {
    wake?.();
    wake = null;
  };
  m(mocks.queryClient, "follow").mockImplementation(() =>
    (async function* () {
      for (;;) {
        const next = queue.shift();
        if (next) {
          yield next;
          continue;
        }
        if (failure) throw failure;
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    })(),
  );
  return {
    push: async (n: number) => {
      await act(async () => {
        queue.push({ records: Array.from({ length: n }, (_, i) => ({ raw: new Uint8Array([i]) })) });
        notify();
        await Promise.resolve();
      });
    },
    fail: async (err: Error) => {
      await act(async () => {
        failure = err;
        notify();
        await Promise.resolve();
      });
    },
  };
}

type Props = { maxRecords: number; onError?: (err: Error) => void };

beforeEach(() => {
  m(mocks.queryClient, "follow").mockReset();
});

describe("useFollow", () => {
  test("the buffer keeps at most maxRecords records", async () => {
    const stream = serveFollowStream();
    const { result, unmount } = renderHook(() => useFollow({ maxRecords: 3 }));

    act(() => {
      result.current.follow("error");
    });
    await stream.push(5);

    await waitFor(() => expect(result.current.records).toHaveLength(3));
    unmount();
  });

  test("maxRecords from a render React threw away never trims the buffer", async () => {
    const stream = serveFollowStream();
    const { result, abandon, unmount } = renderHookWithAbandonedRender(
      (props: Props) => useFollow(props),
      { maxRecords: 3 },
    );

    act(() => {
      result.current.follow("error");
    });
    await abandon({ maxRecords: 1 });
    await stream.push(3);

    await waitFor(() => expect(result.current.records).toHaveLength(3));
    unmount();
  });

  test("an invalid query stops following and reports to the latest onError", async () => {
    const stream = serveFollowStream();
    const first = mock((_err: Error) => {});
    const latest = mock((_err: Error) => {});
    const { result, rerender, unmount } = renderHook((props: Props) => useFollow(props), {
      initialProps: { maxRecords: 10, onError: first },
    });
    rerender({ maxRecords: 10, onError: latest });

    act(() => {
      result.current.follow("bad |");
    });
    await stream.fail(new ConnectError("bad query", Code.InvalidArgument));

    expect(latest).toHaveBeenCalledTimes(1);
    expect(first).not.toHaveBeenCalled();
    expect(result.current.isFollowing).toBe(false);
    unmount();
  });

  test("an error never reaches the onError of a render React threw away", async () => {
    const stream = serveFollowStream();
    const committed = mock((_err: Error) => {});
    const abandoned = mock((_err: Error) => {});
    const { result, abandon, unmount } = renderHookWithAbandonedRender(
      (props: Props) => useFollow(props),
      { maxRecords: 10, onError: committed },
    );

    act(() => {
      result.current.follow("bad |");
    });
    await abandon({ maxRecords: 10, onError: abandoned });
    await stream.fail(new ConnectError("bad query", Code.InvalidArgument));

    expect(abandoned).not.toHaveBeenCalled();
    expect(committed).toHaveBeenCalledTimes(1);
    unmount();
  });
});
