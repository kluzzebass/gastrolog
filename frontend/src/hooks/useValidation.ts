import { useState, useEffect, useRef, useTransition } from "react";
import { queryClient } from "../api/client";

interface ValidationResult {
  valid: boolean;
  errorMessage: string | null;
  errorOffset: number; // -1 if valid
}

const VALID: ValidationResult = { valid: true, errorMessage: null, errorOffset: -1 };

/**
 * Calls the backend ValidateQuery RPC with debouncing.
 * Returns the validation result for the current expression.
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
  }, [expression]); // eslint-disable-line react-hooks/exhaustive-deps

  return result;
}
