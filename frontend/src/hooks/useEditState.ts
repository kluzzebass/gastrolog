import { useState, useCallback } from "react";

interface Edit<T> {
  value: T;
  // JSON of defaults(id) when the edit began. Once defaults(id) diverges
  // (e.g. config updated via WatchConfig or another mutation), the edit is
  // stale: getEdit returns the fresh defaults and the next setEdit starts
  // over from them.
  baseline: string;
}

function liveEdit<T>(edits: Record<string, Edit<T>>, id: string, base: T): T | undefined {
  const edit = edits[id];
  if (!edit) return undefined;
  return edit.baseline === JSON.stringify(base) ? edit.value : undefined;
}

export function useEditState<T extends Record<string, any>>(
  defaults: (id: string) => T,
) {
  const [edits, setEdits] = useState<Record<string, Edit<T>>>({});

  const getEdit = useCallback(
    (id: string): T => {
      const base = defaults(id);
      return liveEdit(edits, id, base) ?? base;
    },
    [edits, defaults],
  );

  const setEdit = useCallback(
    (id: string, patch: Partial<T>) => {
      const base = defaults(id);
      setEdits((prev) => {
        const current = liveEdit(prev, id, base) ?? base;
        return {
          ...prev,
          [id]: { value: { ...current, ...patch }, baseline: JSON.stringify(base) },
        };
      });
    },
    [defaults],
  );

  const clearEdit = useCallback((id: string) => {
    setEdits((prev) => {
      if (!prev[id]) return prev;
      const next = { ...prev };
      delete next[id];
      return next;
    });
  }, []);

  const isDirty = useCallback(
    (id: string): boolean => {
      const base = defaults(id);
      const edit = liveEdit(edits, id, base);
      return edit !== undefined && JSON.stringify(edit) !== JSON.stringify(base);
    },
    [edits, defaults],
  );

  return { getEdit, setEdit, clearEdit, isDirty };
}
