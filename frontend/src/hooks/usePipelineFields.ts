import { useState, useEffect, useRef, useTransition } from "react";
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
 * cursor and baseFields are read from refs at fire time so they
 * don't reset the debounce timer on every keystroke / cursor move.
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

  // Keep latest values in refs — read when the timer fires, not as deps.
  const cursorRef = useRef(cursor);
  cursorRef.current = cursor;
  const baseFieldsRef = useRef(baseFields);
  baseFieldsRef.current = baseFields;

  useEffect(() => {
    clearTimeout(debounceRef.current);

    // Only call when there's a pipe in the expression.
    if (!expression.includes("|")) {
      startTransition(() => setResult(EMPTY));
      return;
    }

    debounceRef.current = setTimeout(async () => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const resp = await queryClient.getPipelineFields(
          {
            expression,
            cursor: cursorRef.current,
            baseFields: baseFieldsRef.current,
          },
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
    }, 300);

    return () => {
      clearTimeout(debounceRef.current);
    };
  }, [expression]); // eslint-disable-line react-hooks/exhaustive-deps

  return result;
}
