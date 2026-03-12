import { useState, useRef, type MutableRefObject } from "react";
import { ConnectError, Code } from "@connectrpc/connect";
import { queryClient, Query, Record, TableResult, refreshAuth } from "../client";
import { HistogramBucket } from "../gen/gastrolog/v1/query_pb";

interface SearchState {
  records: Record[];
  isSearching: boolean;
  error: Error | null;
  hasMore: boolean;
  resumeToken: Uint8Array | null;
  tableResult: TableResult | null;
  histogram: HistogramBucket[] | null;
  version: number;
}

const OPERATORS = new Set(["AND", "OR", "NOT"]);
const DIRECTIVE_PREFIXES = ["reverse=", "start=", "end=", "last=", "vault_id=", "limit="];

function stripPipeline(queryStr: string): string {
  let inQuote: string | null = null;
  for (let i = 0; i < queryStr.length; i++) {
    const ch = queryStr[i]!;
    if (inQuote) {
      if (ch === "\\" && i + 1 < queryStr.length) i++;
      else if (ch === inQuote) inQuote = null;
    } else if (ch === '"' || ch === "'") {
      inQuote = ch;
    } else if (ch === "|") {
      return queryStr.slice(0, i);
    }
  }
  return queryStr;
}

function isDirective(lower: string): boolean {
  return DIRECTIVE_PREFIXES.some((p) => lower.startsWith(p));
}

function tokenFromPart(part: string): string | null {
  part = part.replace(/[()]/g, "");
  if (!part) return null;
  if (OPERATORS.has(part.toUpperCase())) return null;
  const lower = part.toLowerCase();
  if (isDirective(lower)) return null;
  if (part.includes("=")) {
    const value = part.slice(part.indexOf("=") + 1);
    return value && value !== "*" ? value.toLowerCase() : null;
  }
  return lower;
}

/**
 * Extract bare-word tokens from a query string for UI highlighting.
 * This is a lightweight client-side extraction only — the server owns
 * all real query parsing via the `expression` field.
 */
export function extractTokens(queryStr: string): string[] {
  const filterPart = stripPipeline(queryStr);
  const parts = filterPart.trim().split(/\s+/).filter(Boolean);
  const tokens: string[] = [];
  for (const part of parts) {
    const token = tokenFromPart(part);
    if (token) tokens.push(token);
  }
  return tokens;
}

export function useSearch(options?: { onError?: (err: Error) => void }) {
  const onErrorRef = useRef(options?.onError) as MutableRefObject<((err: Error) => void) | undefined>;
  onErrorRef.current = options?.onError;

  const [state, setState] = useState<SearchState>({
    records: [],
    isSearching: false,
    error: null,
    hasMore: false,
    resumeToken: null,
    tableResult: null,
    histogram: null,
    version: 0,
  });

  const abortRef = useRef<AbortController | null>(null);
  // Ref mirrors state so callbacks always read fresh values (no stale closures).
  const stateRef = useRef(state);
  stateRef.current = state;

  // eslint-disable-next-line sonarjs/cognitive-complexity -- streaming search with append/silent/auth-retry
  const search = async (queryStr: string, append = false, keepPrevious = false, silent = false) => {
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

      // Silent mode: don't touch state until the full response arrives.
      // Used by poll/auto-refresh to avoid UI jank.
      if (!silent) {
        setState((prev) => ({
          ...prev,
          isSearching: true,
          error: null,
          records: append || keepPrevious ? prev.records : [],
          resumeToken: append ? prev.resumeToken : null,
          // Only preserve tableResult on append (infinite scroll).
          // keepPrevious preserves records for smooth transitions, but stale
          // pipeline results must not bleed into a new non-pipeline search.
          tableResult: append ? prev.tableResult : null,
          histogram: append ? prev.histogram : null,
        }));
      }

      try {
        const cur = stateRef.current;
        const allRecords: Record[] = append ? [...cur.records] : [];
        let lastResumeToken: Uint8Array<ArrayBuffer> | null = append
          ? (cur.resumeToken as Uint8Array<ArrayBuffer> | null)
          : null;
        let hasMore = false;
        let histogram: HistogramBucket[] | null = null;

        // Stream results
        for await (const response of queryClient.search(
          {
            query,
            resumeToken: lastResumeToken ?? new Uint8Array(0),
          },
          { signal: abortRef.current.signal },
        )) {
          // Capture histogram from whichever response carries it.
          if (response.histogram.length > 0) {
            histogram = response.histogram;
          }

          // Pipeline queries return a single response with tableResult.
          if (response.tableResult) {
            setState((prev) => ({
              ...prev,
              tableResult: response.tableResult ?? null,
              histogram,
              isSearching: false,
              hasMore: false,
              resumeToken: null,
              version: prev.version + 1,
            }));
            abortRef.current = null;
            return;
          }

          allRecords.push(...response.records);
          lastResumeToken =
            response.resumeToken.length > 0 ? response.resumeToken : null;
          hasMore = response.hasMore;

          // In silent mode, buffer everything and swap at the end.
          if (!silent) {
            // Update state incrementally as records arrive
            setState((prev) => ({
              ...prev,
              records: [...allRecords],
              tableResult: null,
              hasMore,
              resumeToken: lastResumeToken,
            }));
          }
        }

        abortRef.current = null;
        setState((prev) => ({
          ...prev,
          // In silent mode, this is the single atomic swap.
          records: silent ? [...allRecords] : prev.records,
          isSearching: false,
          hasMore,
          resumeToken: lastResumeToken,
          histogram,
          version: prev.version + 1,
        }));
      } catch (err) {
        if (
          (err instanceof Error && err.name === "AbortError") ||
          (err instanceof Error && err.message.includes("aborted"))
        ) {
          // Search was cancelled, ignore
          return;
        }
        // Unauthenticated during streaming (e.g. token expired while tab
        // was backgrounded): silently refresh and retry.
        if (
          err instanceof ConnectError &&
          err.code === Code.Unauthenticated
        ) {
          abortRef.current = null;
          const refreshed = await refreshAuth();
          if (refreshed) {
            search(queryStr, append, keepPrevious, silent);
            return;
          }
          // Refresh failed — don't surface error, interceptor will
          // redirect to login on the next request.
          setState((prev) => ({ ...prev, isSearching: false }));
          return;
        }
        abortRef.current = null;
        const error = err instanceof Error ? err : new Error(String(err));
        setState((prev) => ({
          ...prev,
          isSearching: false,
          hasMore: false,
          resumeToken: null,
          error,
        }));
        onErrorRef.current?.(error);
      }
  };

  const loadMore = (queryStr: string) => {
    const cur = stateRef.current;
    if (cur.hasMore && cur.resumeToken) {
      search(queryStr, true);
    }
  };

  const reset = () => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    setState({
      records: [],
      isSearching: false,
      error: null,
      hasMore: false,
      resumeToken: null,
      tableResult: null,
      histogram: null,
      version: 0,
    });
  };

  // Adopt externally-provided records (e.g. from follow mode).
  // Sets records without executing a search.
  const setRecords = (records: Record[]) => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setState((prev) => ({
      records,
      isSearching: false,
      error: null,
      hasMore: false,
      resumeToken: null,
      tableResult: null,
      histogram: null,
      version: prev.version + 1,
    }));
  };

  const cancel = () => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setState((prev) => ({ ...prev, isSearching: false }));
  };

  return {
    ...state,
    search,
    loadMore,
    reset,
    cancel,
    setRecords,
  };
}
