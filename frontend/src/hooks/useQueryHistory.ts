import { useState, useCallback, useEffect } from "react";

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

  useEffect(() => {
    if (entries.length === 0) {
      localStorage.removeItem(STORAGE_KEY);
    } else {
      save(entries);
    }
  }, [entries]);

  const add = useCallback((rawQuery: string) => {
    const query = rawQuery.trim();
    if (!query) return;

    setEntries((prev) => {
      // Remove duplicate if it already exists.
      const filtered = prev.filter((e) => e.query !== query);
      return [{ query, timestamp: Date.now() }, ...filtered].slice(
        0,
        MAX_ENTRIES,
      );
    });
  }, []);

  const remove = useCallback((query: string) => {
    setEntries((prev) => prev.filter((e) => e.query !== query));
  }, []);

  const clear = useCallback(() => {
    setEntries([]);
  }, []);

  return { entries, add, remove, clear };
}
