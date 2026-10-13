import { describe, test, expect, mock, beforeEach } from "bun:test";
import { renderHook, act, waitFor } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { renderHookWithAbandonedRender } from "../../../test/abandonedRender";

const mocks = installMockClients();

import { useWatchJobs } from "./useJobs";

/** A watchJobs stream that stays open until the test drops it. */
function serveJobsStream() {
  let drop: ((err: Error) => void) | null = null;
  m(mocks.jobClient, "watchJobs").mockImplementation(() =>
    (async function* () {
      yield* [];
      await new Promise<never>((_resolve, reject) => {
        drop = reject;
      });
    })(),
  );
  return {
    drop: async (err: Error) => {
      await waitFor(() => expect(drop).not.toBeNull());
      await act(async () => {
        drop?.(err);
        await Promise.resolve();
      });
    },
  };
}

type Props = { onError: (err: Error) => void };

beforeEach(() => {
  m(mocks.jobClient, "watchJobs").mockReset();
});

describe("useWatchJobs", () => {
  test("a dropped stream reports to the latest onError and reconnects", async () => {
    const stream = serveJobsStream();
    const first = mock((_err: Error) => {});
    const latest = mock((_err: Error) => {});
    const { result, rerender, unmount } = renderHook((props: Props) => useWatchJobs(props), {
      initialProps: { onError: first },
    });
    rerender({ onError: latest });

    const dropped = new Error("connection reset");
    await stream.drop(dropped);

    expect(latest).toHaveBeenCalledWith(dropped);
    expect(first).not.toHaveBeenCalled();
    expect(result.current.reconnecting).toBe(true);
    unmount();
  });

  test("an error never reaches the onError of a render React threw away", async () => {
    const stream = serveJobsStream();
    const committed = mock((_err: Error) => {});
    const abandoned = mock((_err: Error) => {});
    const { abandon, unmount } = renderHookWithAbandonedRender(
      (props: Props) => useWatchJobs(props),
      { onError: committed },
    );

    await abandon({ onError: abandoned });
    await stream.drop(new Error("connection reset"));

    expect(abandoned).not.toHaveBeenCalled();
    expect(committed).toHaveBeenCalledTimes(1);
    unmount();
  });
});
