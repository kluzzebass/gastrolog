import { useState, useCallback } from "react";
import { queryClient, Query, ChunkPlan } from "../client";

export interface ExplainState {
  chunks: ChunkPlan[];
  direction: string;
  totalChunks: number;
  expression: string;
  isLoading: boolean;
  error: Error | null;
}

export function useExplain() {
  const [state, setState] = useState<ExplainState>({
    chunks: [],
    direction: "",
    totalChunks: 0,
    expression: "",
    isLoading: false,
    error: null,
  });

  const explain = useCallback(async (queryStr: string) => {
    // Send the raw query string — the server parses it.
    const query = new Query();
    query.expression = queryStr;

    setState((prev) => ({
      ...prev,
      isLoading: true,
      error: null,
    }));

    try {
      const response = await queryClient.explain({ query });
      setState({
        chunks: response.chunks,
        direction: response.direction,
        totalChunks: response.totalChunks,
        expression: response.expression,
        isLoading: false,
        error: null,
      });
    } catch (err) {
      setState({
        chunks: [],
        direction: "",
        totalChunks: 0,
        expression: "",
        isLoading: false,
        error: err instanceof Error ? err : new Error(String(err)),
      });
    }
  }, []);

  const reset = useCallback(() => {
    setState({
      chunks: [],
      direction: "",
      totalChunks: 0,
      expression: "",
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
