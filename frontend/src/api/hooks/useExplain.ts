import { useState, useCallback, useRef, type MutableRefObject } from "react";
import { queryClient, Query, ChunkPlan, QueryPipelineStage } from "../client";
import { ContributionReport } from "../gen/gastrolog/v1/vault_pb";

interface ExplainState {
  chunks: ChunkPlan[];
  direction: string;
  totalChunks: number;
  expression: string;
  pipelineStages: QueryPipelineStage[];
  // Present only when the cross-node plan fan-out could not reach every
  // peer, so the plan omits some node's chunks (gastrolog-1ic07).
  contributionReport: ContributionReport | null;
  isLoading: boolean;
  error: Error | null;
}

const emptyExplainState: ExplainState = {
  chunks: [],
  direction: "",
  totalChunks: 0,
  expression: "",
  pipelineStages: [],
  contributionReport: null,
  isLoading: false,
  error: null,
};

export function useExplain(options?: { onError?: (err: Error) => void }) {
  const onErrorRef = useRef(options?.onError) as MutableRefObject<((err: Error) => void) | undefined>;
  onErrorRef.current = options?.onError;

  const [state, setState] = useState<ExplainState>(emptyExplainState);

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
        pipelineStages: response.pipelineStages,
        contributionReport: response.contributionReport ?? null,
        isLoading: false,
        error: null,
      });
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      setState({ ...emptyExplainState, error });
      onErrorRef.current?.(error);
    }
  }, []);

  const reset = useCallback(() => {
    setState(emptyExplainState);
  }, []);

  return {
    ...state,
    explain,
    reset,
  };
}
