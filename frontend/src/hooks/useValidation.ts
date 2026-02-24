import { useState, useEffect, useRef } from "react";
import { queryClient } from "../api/client";

interface ValidationResult {
  valid: boolean;
  errorMessage: string | null;
  errorOffset: number; // -1 if valid
}

/**
 * Calls the backend ValidateQuery RPC with debouncing.
 * Returns the validation result for the current expression.
 */
export function useValidation(expression: string): ValidationResult {
  const [result, setResult] = useState<ValidationResult>({
    valid: true,
    errorMessage: null,
    errorOffset: -1,
  });
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    clearTimeout(debounceRef.current);

    // Empty expression is always valid.
    if (!expression.trim()) {
      setResult({ valid: true, errorMessage: null, errorOffset: -1 });
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
          setResult({
            valid: resp.valid,
            errorMessage: resp.errorMessage || null,
            errorOffset: resp.errorOffset,
          });
        }
      } catch {
        // Network error or abort — keep last result.
      }
    }, 100);

    return () => {
      clearTimeout(debounceRef.current);
    };
  }, [expression]);

  return result;
}
