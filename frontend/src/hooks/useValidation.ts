import { useState, useEffect, useRef, useTransition } from "react";
import { queryClient } from "../api/client";

export interface ValidationResult {
  valid: boolean;
  errorMessage: string | null;
  errorOffset: number; // -1 if valid
  spans: Array<{ text: string; role: string }>; // empty = no response yet
  expression: string; // what expression these spans are for
  hasPipeline: boolean;
}

const VALID: ValidationResult = {
  valid: true,
  errorMessage: null,
  errorOffset: -1,
  spans: [],
  expression: "",
  hasPipeline: false,
};

/**
 * Calls the backend ValidateQuery RPC with debouncing.
 * Returns the validation result for the current expression,
 * including syntax highlighting spans.
 *
 * State updates use startTransition so they never block user input.
 */
export function useValidation(expression: string): ValidationResult {
  const [result, setResult] = useState<ValidationResult>(VALID);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const abortRef = useRef<AbortController | null>(null);
  const [, startTransition] = useTransition();

  useEffect(() => {
    clearTimeout(debounceRef.current);

    // Empty expression is always valid.
    if (!expression.trim()) {
      setResult(VALID);
      return;
    }

    debounceRef.current = setTimeout(async () => {
      // Cancel any in-flight request.
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const resp = await queryClient.validateQuery(
          { expression },
          { signal: controller.signal },
        );
        if (!controller.signal.aborted) {
          startTransition(() => {
            setResult({
              valid: resp.valid,
              errorMessage: resp.errorMessage || null,
              errorOffset: resp.errorOffset,
              spans: resp.spans.map((s) => ({ text: s.text, role: s.role })),
              expression: resp.expression,
              hasPipeline: resp.hasPipeline,
            });
          });
        }
      } catch {
        // Network error or abort — keep last result.
      }
    }, 200);

    return () => {
      clearTimeout(debounceRef.current);
    };
  }, [expression]);

  return result;
}
