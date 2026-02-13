import { useState, useCallback } from "react";

export function useEditState<T extends Record<string, unknown>>(
  defaults: (id: string) => T,
) {
  const [edits, setEdits] = useState<Record<string, T>>({});

  const getEdit = useCallback(
    (id: string): T => {
      return edits[id] ?? defaults(id);
    },
    [edits, defaults],
  );

  const setEdit = useCallback(
    (id: string, patch: Partial<T>) => {
      setEdits((prev) => ({
        ...prev,
        [id]: { ...defaults(id), ...prev[id], ...patch } as T,
      }));
    },
    [defaults],
  );

  const clearEdit = useCallback((id: string) => {
    setEdits((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });
  }, []);

  return { getEdit, setEdit, clearEdit };
}
