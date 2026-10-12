import { describe, test, expect, mock, beforeEach, afterEach } from "bun:test";
import { renderHook } from "@testing-library/react";
import { renderHookWithAbandonedRender } from "../../test/abandonedRender";
import { usePolling } from "./usePolling";

describe("usePolling", () => {
  let realSetInterval: typeof globalThis.setInterval;
  let realClearInterval: typeof globalThis.clearInterval;
  let intervalCallbacks: Map<number, () => void>;
  let nextId: number;
  let clearCount: number;

  const tick = () => {
    for (const cb of intervalCallbacks.values()) cb();
  };

  beforeEach(() => {
    realSetInterval = globalThis.setInterval;
    realClearInterval = globalThis.clearInterval;
    intervalCallbacks = new Map();
    nextId = 1;
    clearCount = 0;

    globalThis.setInterval = ((fn: () => void, _ms?: number) => {
      const id = nextId++;
      intervalCallbacks.set(id, fn);
      return id;
    }) as typeof setInterval;

    globalThis.clearInterval = ((id: number) => {
      intervalCallbacks.delete(id);
      clearCount++;
    }) as typeof clearInterval;
  });

  afterEach(() => {
    globalThis.setInterval = realSetInterval;
    globalThis.clearInterval = realClearInterval;
  });

  test("polls once immediately when polling starts", () => {
    const onPoll = mock(() => {});
    const { rerender } = renderHook(({ ms }) => usePolling(ms, onPoll), {
      initialProps: { ms: null as number | null },
    });
    expect(onPoll).not.toHaveBeenCalled();

    rerender({ ms: 5000 });
    expect(onPoll).toHaveBeenCalledTimes(1);
    expect(intervalCallbacks.size).toBe(1);
  });

  test("a cadence change restarts the interval and polls immediately", () => {
    const onPoll = mock(() => {});
    const { rerender } = renderHook(({ ms }) => usePolling(ms, onPoll), {
      initialProps: { ms: 5000 as number | null },
    });
    rerender({ ms: 10_000 });
    expect(onPoll).toHaveBeenCalledTimes(2);
    expect(intervalCallbacks.size).toBe(1);
  });

  test("re-renders with a new callback keep the interval and tick the latest callback", () => {
    const first = mock(() => {});
    const { rerender } = renderHook(({ fn }) => usePolling(5000, fn), {
      initialProps: { fn: first },
    });
    const initialClearCount = clearCount;

    let latest = first;
    for (let i = 0; i < 10; i++) {
      latest = mock(() => {});
      rerender({ fn: latest });
    }

    expect(clearCount).toBe(initialClearCount);
    expect(intervalCallbacks.size).toBe(1);

    tick();
    expect(latest).toHaveBeenCalledTimes(1);
    expect(first).toHaveBeenCalledTimes(1); // the leading poll only
  });

  test("null stops polling", () => {
    const onPoll = mock(() => {});
    const { rerender } = renderHook(({ ms }) => usePolling(ms, onPoll), {
      initialProps: { ms: 5000 as number | null },
    });
    expect(intervalCallbacks.size).toBe(1);

    rerender({ ms: null });
    expect(intervalCallbacks.size).toBe(0);
  });

  test("a tick never calls the callback of a render React threw away", async () => {
    const committed = mock(() => {});
    const abandoned = mock(() => {});
    const { abandon } = renderHookWithAbandonedRender((fn: () => void) => usePolling(5000, fn), committed);

    await abandon(abandoned);
    tick();

    expect(abandoned).not.toHaveBeenCalled();
    expect(committed).toHaveBeenCalledTimes(2);
  });
});
