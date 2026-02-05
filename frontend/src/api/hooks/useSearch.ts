import { useState, useCallback, useRef } from "react";
import { queryClient, Query, Record, KVPredicate } from "../client";
import { Timestamp } from "@bufbuild/protobuf";

export interface SearchState {
  records: Record[];
  isSearching: boolean;
  error: Error | null;
  hasMore: boolean;
  resumeToken: Uint8Array | null;
}

export interface ParsedQuery {
  tokens: string[];
  kvPredicates: { key: string; value: string }[];
  start?: Date;
  end?: Date;
  limit?: number;
}

/**
 * Parse a query string into structured query parts.
 * Syntax:
 *   - Bare words are tokens: "error timeout"
 *   - key=value are KV predicates: "level=ERROR service=payment"
 *   - start=ISO8601 sets start time
 *   - end=ISO8601 sets end time
 *   - limit=N sets max results
 */
export function parseQuery(queryStr: string): ParsedQuery {
  const parts = queryStr.trim().split(/\s+/).filter(Boolean);
  const tokens: string[] = [];
  const kvPredicates: { key: string; value: string }[] = [];
  let start: Date | undefined;
  let end: Date | undefined;
  let limit: number | undefined;

  for (const part of parts) {
    if (part.includes("=")) {
      const eqIdx = part.indexOf("=");
      const key = part.slice(0, eqIdx).toLowerCase();
      const value = part.slice(eqIdx + 1);

      if (key === "start") {
        const ts = Date.parse(value);
        if (!isNaN(ts)) start = new Date(ts);
      } else if (key === "end") {
        const ts = Date.parse(value);
        if (!isNaN(ts)) end = new Date(ts);
      } else if (key === "limit") {
        const n = parseInt(value, 10);
        if (!isNaN(n) && n > 0) limit = n;
      } else {
        kvPredicates.push({ key, value });
      }
    } else {
      tokens.push(part.toLowerCase());
    }
  }

  return { tokens, kvPredicates, start, end, limit };
}

/**
 * Build a Query protobuf message from parsed query parts.
 */
export function buildQuery(parsed: ParsedQuery): Query {
  const query = new Query();
  query.tokens = parsed.tokens;
  query.kvPredicates = parsed.kvPredicates.map((kv) => {
    const pred = new KVPredicate();
    pred.key = kv.key;
    pred.value = kv.value;
    return pred;
  });
  if (parsed.start) {
    query.start = Timestamp.fromDate(parsed.start);
  }
  if (parsed.end) {
    query.end = Timestamp.fromDate(parsed.end);
  }
  if (parsed.limit) {
    query.limit = BigInt(parsed.limit);
  }
  return query;
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
      // Cancel any in-flight request
      if (abortRef.current) {
        abortRef.current.abort();
      }
      abortRef.current = new AbortController();

      const parsed = parseQuery(queryStr);
      const query = buildQuery(parsed);

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

        setState((prev) => ({
          ...prev,
          isSearching: false,
          hasMore,
          resumeToken: lastResumeToken,
        }));
      } catch (err) {
        if (err instanceof Error && err.name === "AbortError") {
          // Search was cancelled, ignore
          return;
        }
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
