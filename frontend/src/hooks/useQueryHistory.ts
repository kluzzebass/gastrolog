import { useState, useCallback } from "react";

const STORAGE_KEY = "gastrolog:query-history";
const MAX_ENTRIES = 50;

export interface HistoryEntry {
  query: string;
  timestamp: number;
}

function load(): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    return JSON.parse(raw) as HistoryEntry[];
  } catch {
    return [];
  }
}

function save(entries: HistoryEntry[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(entries));
}

export function useQueryHistory() {
  const [entries, setEntries] = useState<HistoryEntry[]>(load);

  const add = useCallback((rawQuery: string) => {
    const query = rawQuery.trim();
    if (!query) return;

    setEntries((prev) => {
      // Remove duplicate if it already exists.
      const filtered = prev.filter((e) => e.query !== query);
      const next = [{ query, timestamp: Date.now() }, ...filtered].slice(
        0,
        MAX_ENTRIES,
      );
      save(next);
      return next;
    });
  }, []);

  const remove = useCallback((query: string) => {
    setEntries((prev) => {
      const next = prev.filter((e) => e.query !== query);
      save(next);
      return next;
    });
  }, []);

  const clear = useCallback(() => {
    setEntries([]);
    localStorage.removeItem(STORAGE_KEY);
  }, []);

  return { entries, add, remove, clear };
}
