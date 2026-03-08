import { useState, useEffect, useRef, useTransition } from "react";
import { queryClient } from "../api/client";
import type { FieldSummary } from "../utils";
import type { FieldInfo } from "../api/gen/gastrolog/v1/query_pb";

interface FieldsResult {
  attrFields: FieldSummary[];
  kvFields: FieldSummary[];
}

const EMPTY: FieldsResult = { attrFields: [], kvFields: [] };

function protoToSummary(fields: FieldInfo[]): FieldSummary[] {
  return fields.map((f) => ({
    key: f.key,
    count: f.count,
    values: f.topValues.map((v) => ({ value: v.value, count: v.count })),
  }));
}

/**
 * Calls the backend GetFields RPC with debouncing to discover field names
 * and value distributions from sampled records. Replaces client-side
 * extractKVPairs + aggregateFields with the backend's full extractor suite.
 *
 * Only fires when there are actual search results (hasResults=true) and
 * the query is not a pipeline (pipeline results have no raw records).
 */
export function useFields(
  expression: string,
  hasResults: boolean,
  isPipeline: boolean,
): FieldsResult {
  const [result, setResult] = useState<FieldsResult>(EMPTY);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const abortRef = useRef<AbortController | null>(null);
  const [, startTransition] = useTransition();

  useEffect(() => {
    clearTimeout(debounceRef.current);

    if (!hasResults || isPipeline) {
      startTransition(() => setResult(EMPTY));
      return;
    }

    debounceRef.current = setTimeout(async () => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const resp = await queryClient.getFields(
          { expression },
          { signal: controller.signal },
        );
        if (!controller.signal.aborted) {
          startTransition(() => {
            setResult({
              attrFields: protoToSummary(resp.attrFields),
              kvFields: protoToSummary(resp.kvFields),
            });
          });
        }
      } catch (err) {
        if (err instanceof DOMException && err.name === "AbortError") return;
        console.error("[useFields] GetFields RPC failed:", err);
      }
    }, 500);

    return () => {
      clearTimeout(debounceRef.current);
    };
  }, [expression, hasResults, isPipeline]);

  return result;
}
