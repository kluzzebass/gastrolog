import { useState, useEffect, useEffectEvent, useRef, useTransition } from "react";
import { queryClient } from "../api/client";

interface PipelineFieldsResult {
  fields: string[];
  completions: string[];
}

const EMPTY: PipelineFieldsResult = { fields: [], completions: [] };

/**
 * Calls the backend GetPipelineFields RPC with debouncing.
 * Only fires when expression contains a pipe.
 *
 * cursor and baseFields are read when the timer fires, so they don't
 * reset the debounce timer on every keystroke / cursor move.
 * State updates use startTransition so they never block user input.
 */
export function usePipelineFields(
  expression: string,
  cursor: number,
  baseFields: string[],
): PipelineFieldsResult {
  const [result, setResult] = useState<PipelineFieldsResult>(EMPTY);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const abortRef = useRef<AbortController | null>(null);
  const [, startTransition] = useTransition();

  const fetchFields = useEffectEvent(async (expr: string) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const resp = await queryClient.getPipelineFields(
        { expression: expr, cursor, baseFields },
        { signal: controller.signal },
      );
      if (!controller.signal.aborted) {
        startTransition(() => {
          setResult({
            fields: resp.fields,
            completions: resp.completions,
          });
        });
      }
    } catch {
      // Network error or abort — keep last result.
    }
  });

  useEffect(() => {
    clearTimeout(debounceRef.current);

    // Only call when there's a pipe in the expression.
    if (!expression.includes("|")) {
      startTransition(() => setResult(EMPTY));
      return;
    }

    debounceRef.current = setTimeout(() => fetchFields(expression), 300);

    return () => {
      clearTimeout(debounceRef.current);
    };
  }, [expression]);

  return result;
}
