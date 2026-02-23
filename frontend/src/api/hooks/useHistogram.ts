import { useState, useCallback, useRef } from "react";
import { queryClient, Query } from "../client";

export interface HistogramData {
  buckets: {
    ts: Date;
    count: number;
    levelCounts: Record<string, number>;
  }[];
  start: Date | null;
  end: Date | null;
}

export interface HistogramState {
  data: HistogramData | null;
  isLoading: boolean;
  error: Error | null;
}

export function useHistogram() {
  const [state, setState] = useState<HistogramState>({
    data: null,
    isLoading: false,
    error: null,
  });

  const abortRef = useRef<AbortController | null>(null);

  const fetchHistogram = useCallback(
    async (expression: string, numBuckets = 50) => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
      abortRef.current = new AbortController();

      setState((prev) => ({ ...prev, isLoading: true, error: null }));

      try {
        // Build a pipeline query: append "| timechart N" to the expression.
        // Empty expression becomes "| timechart N" (bare pipe = match-all).
        const timechartExpr = expression.trim()
          ? `${expression.trim()} | timechart ${numBuckets}`
          : `| timechart ${numBuckets}`;

        const query = new Query();
        query.expression = timechartExpr;

        // Stream the search response — timechart returns a single response
        // with tableResult containing columns ["_time", "level", "count"].
        let tableResult: { columns: string[]; rows: { values: string[] }[] } | null = null;

        for await (const response of queryClient.search(
          { query, resumeToken: new Uint8Array(0) },
          { signal: abortRef.current.signal },
        )) {
          if (response.tableResult) {
            tableResult = response.tableResult;
            break;
          }
        }

        if (!tableResult) {
          setState({ data: null, isLoading: false, error: null });
          abortRef.current = null;
          return;
        }

        // Transform TableResult (columns: _time, level, count) into HistogramData.
        // Rows are per bucket × level; group by _time to build buckets.
        const timeIdx = tableResult.columns.indexOf("_time");
        const levelIdx = tableResult.columns.indexOf("level");
        const countIdx = tableResult.columns.indexOf("count");

        if (timeIdx < 0 || countIdx < 0) {
          setState({ data: null, isLoading: false, error: null });
          abortRef.current = null;
          return;
        }

        // Group rows by timestamp.
        const bucketMap = new Map<string, { count: number; levelCounts: Record<string, number> }>();

        for (const row of tableResult.rows) {
          const tsStr = row.values[timeIdx]!;
          const level = levelIdx >= 0 ? row.values[levelIdx]! : "";
          const count = Number(row.values[countIdx]!);

          let bucket = bucketMap.get(tsStr);
          if (!bucket) {
            bucket = { count: 0, levelCounts: {} };
            bucketMap.set(tsStr, bucket);
          }
          bucket.count += count;
          if (level) {
            bucket.levelCounts[level] = (bucket.levelCounts[level] ?? 0) + count;
          }
        }

        // Convert to sorted array.
        const buckets = Array.from(bucketMap.entries())
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([tsStr, data]) => ({
            ts: new Date(tsStr),
            count: data.count,
            levelCounts: data.levelCounts,
          }));

        const start = buckets.length > 0 ? buckets[0]!.ts : null;
        const end = buckets.length > 0 ? buckets[buckets.length - 1]!.ts : null;

        setState({
          data: { buckets, start, end },
          isLoading: false,
          error: null,
        });
      } catch (err) {
        if (
          (err instanceof Error && err.name === "AbortError") ||
          (err instanceof Error && err.message.includes("aborted"))
        ) {
          return;
        }
        setState((prev) => ({
          ...prev,
          isLoading: false,
          error: err instanceof Error ? err : new Error(String(err)),
        }));
      }
      abortRef.current = null;
    },
    [],
  );

  const reset = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    setState({ data: null, isLoading: false, error: null });
  }, []);

  return {
    ...state,
    fetchHistogram,
    reset,
  };
}
