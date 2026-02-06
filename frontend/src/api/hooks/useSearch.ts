import { useState, useCallback, useRef } from "react";
import { queryClient, Query, Record } from "../client";

export interface SearchState {
  records: Record[];
  isSearching: boolean;
  error: Error | null;
  hasMore: boolean;
  resumeToken: Uint8Array | null;
}

/**
 * Extract bare-word tokens from a query string for UI highlighting.
 * This is a lightweight client-side extraction only — the server owns
 * all real query parsing via the `expression` field.
 */
export function extractTokens(queryStr: string): string[] {
  const parts = queryStr.trim().split(/\s+/).filter(Boolean);
  const tokens: string[] = [];

  for (let part of parts) {
    // Strip parentheses
    part = part.replace(/[()]/g, "");
    if (!part) continue;

    // Skip operators
    const upper = part.toUpperCase();
    if (upper === "AND" || upper === "OR" || upper === "NOT") {
      continue;
    }

    // Skip query directives (not searchable content)
    const lower = part.toLowerCase();
    if (
      lower === "reverse" ||
      lower.startsWith("start=") ||
      lower.startsWith("end=") ||
      lower.startsWith("store=") ||
      lower.startsWith("limit=")
    ) {
      continue;
    }

    // Extract values from key=value pairs for highlighting
    if (part.includes("=")) {
      const eqIdx = part.indexOf("=");
      const value = part.slice(eqIdx + 1);
      if (value && value !== "*") {
        tokens.push(value.toLowerCase());
      }
      continue;
    }

    tokens.push(lower);
  }

  return tokens;
}

export function useSearch() {
  const [state, setState] = useState<SearchState>({
    records: [],
    isSearching: false,
    error: null,
    hasMore: false,
    resumeToken: null,
  });

  const abortRef = useRef<AbortController | null>(null);

  const search = useCallback(
    async (queryStr: string, append = false) => {
      // Cancel any in-flight request on new searches (not appends).
      if (abortRef.current) {
        if (!append) {
          abortRef.current.abort();
        } else {
          // Don't start an append while a request is still in flight.
          return;
        }
      }
      abortRef.current = new AbortController();

      // Send the raw query string — the server parses it.
      // limit is set on the proto field (not in expression) to control
      // streaming page size. The server returns a resume token after
      // this many records, enabling infinite scroll.
      const query = new Query();
      query.expression = queryStr;
      query.limit = BigInt(100);

      setState((prev) => ({
        ...prev,
        isSearching: true,
        error: null,
        records: append ? prev.records : [],
        resumeToken: append ? prev.resumeToken : null,
      }));

      try {
        const allRecords: Record[] = append ? [...state.records] : [];
        let lastResumeToken: Uint8Array<ArrayBuffer> | null = append
          ? (state.resumeToken as Uint8Array<ArrayBuffer> | null)
          : null;
        let hasMore = false;

        // Stream results
        for await (const response of queryClient.search(
          {
            query,
            resumeToken: lastResumeToken ?? new Uint8Array(0),
          },
          { signal: abortRef.current.signal },
        )) {
          allRecords.push(...response.records);
          lastResumeToken =
            response.resumeToken.length > 0 ? response.resumeToken : null;
          hasMore = response.hasMore;

          // Update state incrementally as records arrive
          setState((prev) => ({
            ...prev,
            records: [...allRecords],
            hasMore,
            resumeToken: lastResumeToken,
          }));
        }

        abortRef.current = null;
        setState((prev) => ({
          ...prev,
          isSearching: false,
          hasMore,
          resumeToken: lastResumeToken,
        }));
      } catch (err) {
        if (
          (err instanceof Error && err.name === "AbortError") ||
          (err instanceof Error && err.message.includes("aborted"))
        ) {
          // Search was cancelled, ignore
          return;
        }
        abortRef.current = null;
        setState((prev) => ({
          ...prev,
          isSearching: false,
          error: err instanceof Error ? err : new Error(String(err)),
        }));
      }
    },
    [state.records, state.resumeToken],
  );

  const loadMore = useCallback(
    (queryStr: string) => {
      if (state.hasMore && state.resumeToken) {
        search(queryStr, true);
      }
    },
    [search, state.hasMore, state.resumeToken],
  );

  const reset = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    setState({
      records: [],
      isSearching: false,
      error: null,
      hasMore: false,
      resumeToken: null,
    });
  }, []);

  return {
    ...state,
    search,
    loadMore,
    reset,
  };
}
