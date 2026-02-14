import { useState, useCallback, useRef } from "react";

export function useEditState<T extends Record<string, unknown>>(
  defaults: (id: string) => T,
) {
  const [edits, setEdits] = useState<Record<string, T>>({});
  // Track IDs where we're waiting for server data to settle after save.
  // While pending, getEdit returns the last-saved values to avoid flashing
  // stale defaults between clearEdit and query refetch.
  const pendingRef = useRef<Record<string, T>>({});

  const getEdit = useCallback(
    (id: string): T => {
      if (edits[id]) return edits[id];
      if (pendingRef.current[id]) return pendingRef.current[id];
      return defaults(id);
    },
    [edits, defaults],
  );

  const setEdit = useCallback(
    (id: string, patch: Partial<T>) => {
      delete pendingRef.current[id];
      setEdits((prev) => ({
        ...prev,
        [id]: { ...defaults(id), ...prev[id], ...patch } as T,
      }));
    },
    [defaults],
  );

  const clearEdit = useCallback((id: string) => {
    setEdits((prev) => {
      if (!prev[id]) return prev;
      // Stash the current edit values so getEdit doesn't flash stale defaults.
      pendingRef.current[id] = prev[id];
      // Clear on next frame (after query refetch settles).
      requestAnimationFrame(() => {
        delete pendingRef.current[id];
      });
      const next = { ...prev };
      delete next[id];
      return next;
    });
  }, []);

  return { getEdit, setEdit, clearEdit };
}
