import { useState, useCallback, useRef } from "react";
import { queryClient, Record, RecordRef } from "../client";

export interface ContextState {
  before: Record[];
  anchor: Record | null;
  after: Record[];
  isLoading: boolean;
  error: Error | null;
}

export function useRecordContext() {
  const [state, setState] = useState<ContextState>({
    before: [],
    anchor: null,
    after: [],
    isLoading: false,
    error: null,
  });

  const abortRef = useRef<AbortController | null>(null);

  const fetchContext = useCallback(
    async (ref: RecordRef, before = 5, after = 5) => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
      abortRef.current = new AbortController();

      setState((prev) => ({ ...prev, isLoading: true, error: null }));

      try {
        const resp = await queryClient.getContext(
          { ref, before, after },
          { signal: abortRef.current.signal },
        );

        abortRef.current = null;
        setState({
          before: resp.before,
          anchor: resp.anchor ?? null,
          after: resp.after,
          isLoading: false,
          error: null,
        });
      } catch (err) {
        if (err instanceof Error && err.name === "AbortError") {
          return;
        }
        abortRef.current = null;
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
      abortRef.current = null;
    }
    setState({
      before: [],
      anchor: null,
      after: [],
      isLoading: false,
      error: null,
    });
  }, []);

  return { ...state, fetchContext, reset };
}
