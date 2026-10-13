import { useEffect, useEffectEvent } from "react";

/**
 * Calls `onPoll` every `intervalMs` while it is set, and once immediately
 * whenever polling starts or the cadence changes: an operator picking a
 * refresh interval expects movement right away, not after one full tick.
 * Re-renders do not restart the interval; each tick calls the latest
 * `onPoll`.
 */
export function usePolling(intervalMs: number | null, onPoll: () => void) {
  const poll = useEffectEvent(onPoll);

  useEffect(() => {
    if (!intervalMs) return;
    poll();
    const id = setInterval(() => poll(), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
}
