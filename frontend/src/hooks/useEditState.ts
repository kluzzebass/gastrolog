import { useState, useCallback, useRef } from "react";

export function useEditState<T extends Record<string, any>>(
  defaults: (id: string) => T,
) {
  const [edits, setEdits] = useState<Record<string, T>>({});

  // Snapshot of defaults(id) at the time each edit was created.
  // If defaults later diverge (e.g. config updated via WatchConfig or another
  // mutation), the stale edit is discarded so getEdit returns fresh values.
  const baselineRef = useRef<Record<string, string>>({});

  const getEdit = useCallback(
    (id: string): T => {
      if (edits[id]) {
        // Check if defaults changed since the edit was created — if so, the
        // edit is stale (e.g. config was updated externally) and must be discarded.
        const currentDefaults = JSON.stringify(defaults(id));
        if (baselineRef.current[id] !== currentDefaults) {
          // Can't call setEdits during render, but returning defaults is safe.
          // The stale entry will be cleaned up on the next setEdit or clearEdit.
          delete baselineRef.current[id];
          return defaults(id);
        }
        return edits[id];
      }
      return defaults(id);
    },
    [edits, defaults],
  );

  const setEdit = useCallback(
    (id: string, patch: Partial<T>) => {
      setEdits((prev) => {
        const base = defaults(id);
        // Snapshot baseline when creating a new edit, or when a prior stale
        // edit lost its baseline (getEdit invalidation path). Without this,
        // subsequent typing can be ignored because getEdit keeps treating the
        // edit as stale forever.
        if (!prev[id] || !Object.hasOwn(baselineRef.current, id)) {
          baselineRef.current[id] = JSON.stringify(base);
        }
        return {
          ...prev,
          [id]: { ...base, ...prev[id], ...patch } as T,
        };
      });
    },
    [defaults],
  );

  const clearEdit = useCallback((id: string) => {
    delete baselineRef.current[id];
    setEdits((prev) => {
      if (!prev[id]) return prev;
      const next = { ...prev };
      delete next[id];
      return next;
    });
  }, []);

  const isDirty = useCallback(
    (id: string): boolean => {
      if (!edits[id]) return false;
      // If baseline diverged, the edit is stale — not dirty.
      const currentDefaults = JSON.stringify(defaults(id));
      if (baselineRef.current[id] !== currentDefaults) return false;
      return JSON.stringify(edits[id]) !== currentDefaults;
    },
    [edits, defaults],
  );

  return { getEdit, setEdit, clearEdit, isDirty };
}
