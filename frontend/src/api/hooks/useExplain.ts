import { useState, useCallback } from "react";
import { queryClient, ChunkPlan } from "../client";
import { parseQuery, buildQuery } from "./useSearch";

export interface ExplainState {
  chunks: ChunkPlan[];
  isLoading: boolean;
  error: Error | null;
}

export function useExplain() {
  const [state, setState] = useState<ExplainState>({
    chunks: [],
    isLoading: false,
    error: null,
  });

  const explain = useCallback(async (queryStr: string) => {
    const parsed = parseQuery(queryStr);
    const query = buildQuery(parsed);

    setState({
      chunks: [],
      isLoading: true,
      error: null,
    });

    try {
      const response = await queryClient.explain({ query });
      setState({
        chunks: response.chunks,
        isLoading: false,
        error: null,
      });
    } catch (err) {
      setState({
        chunks: [],
        isLoading: false,
        error: err instanceof Error ? err : new Error(String(err)),
      });
    }
  }, []);

  const reset = useCallback(() => {
    setState({
      chunks: [],
      isLoading: false,
      error: null,
    });
  }, []);

  return {
    ...state,
    explain,
    reset,
  };
}
