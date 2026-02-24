import { useState, useEffect, useRef } from "react";
import { queryClient } from "../api/client";

interface PipelineFieldsResult {
  fields: string[];
  completions: string[];
}

/**
 * Calls the backend GetPipelineFields RPC with debouncing.
 * Only fires when expression contains a pipe.
 */
export function usePipelineFields(
  expression: string,
  cursor: number,
  baseFields: string[],
): PipelineFieldsResult {
  const [result, setResult] = useState<PipelineFieldsResult>({
    fields: [],
    completions: [],
  });
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const abortRef = useRef<AbortController | null>(null);

  // Serialize baseFields for dependency tracking.
  const baseFieldsKey = baseFields.join("\0");

  useEffect(() => {
    clearTimeout(debounceRef.current);

    // Only call when there's a pipe in the expression.
    if (!expression.includes("|")) {
      setResult({ fields: [], completions: [] });
      return;
    }

    debounceRef.current = setTimeout(async () => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const resp = await queryClient.getPipelineFields(
          { expression, cursor, baseFields },
          { signal: controller.signal },
        );
        if (!controller.signal.aborted) {
          setResult({
            fields: resp.fields,
            completions: resp.completions,
          });
        }
      } catch {
        // Network error or abort — keep last result.
      }
    }, 150);

    return () => {
      clearTimeout(debounceRef.current);
    };
  }, [expression, cursor, baseFieldsKey]); // eslint-disable-line react-hooks/exhaustive-deps

  return result;
}
