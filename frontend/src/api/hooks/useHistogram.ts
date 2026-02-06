import { useState, useCallback, useRef } from "react";
import { queryClient, HistogramBucket } from "../client";

export interface HistogramData {
  buckets: { ts: Date; count: number }[];
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
        const response = await queryClient.histogram(
          { expression, buckets: numBuckets },
          { signal: abortRef.current.signal },
        );

        const buckets = response.buckets.map((b: HistogramBucket) => ({
          ts: b.ts ? b.ts.toDate() : new Date(),
          count: Number(b.count),
        }));

        setState({
          data: {
            buckets,
            start: response.start ? response.start.toDate() : null,
            end: response.end ? response.end.toDate() : null,
          },
          isLoading: false,
          error: null,
        });
      } catch (err) {
        if (err instanceof Error && err.name === "AbortError") {
          return;
        }
        setState((prev) => ({
          ...prev,
          isLoading: false,
          error: err instanceof Error ? err : new Error(String(err)),
        }));
      }
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
