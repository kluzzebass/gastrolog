import { useState, useCallback, useRef } from "react";
import { queryClient, Query, Record } from "../client";

const MAX_RECORDS = 5000;

export interface FollowState {
  records: Record[];
  isFollowing: boolean;
  error: Error | null;
}

export function useFollow() {
  const [state, setState] = useState<FollowState>({
    records: [],
    isFollowing: false,
    error: null,
  });

  const abortRef = useRef<AbortController | null>(null);

  const follow = useCallback(async (queryStr: string) => {
    // Cancel any previous follow.
    if (abortRef.current) {
      abortRef.current.abort();
    }
    abortRef.current = new AbortController();

    // Strip start=/end= from expression — Follow ignores time bounds.
    const stripped = queryStr
      .replace(/\bstart=\S+/g, "")
      .replace(/\bend=\S+/g, "")
      .replace(/\s+/g, " ")
      .trim();

    const query = new Query();
    query.expression = stripped;

    setState({
      records: [],
      isFollowing: true,
      error: null,
    });

    try {
      const buffer: Record[] = [];

      for await (const response of queryClient.follow(
        { query },
        { signal: abortRef.current.signal },
      )) {
        // Prepend new records so newest appear first.
        buffer.unshift(...response.records);

        // Cap buffer size — drop oldest (at the end).
        if (buffer.length > MAX_RECORDS) {
          buffer.length = MAX_RECORDS;
        }

        setState((prev) => ({
          ...prev,
          records: [...buffer],
        }));
      }

      // Stream ended (shouldn't happen normally).
      abortRef.current = null;
      setState((prev) => ({
        ...prev,
        isFollowing: false,
      }));
    } catch (err) {
      if (
        (err instanceof Error && err.name === "AbortError") ||
        (err instanceof Error && err.message.includes("aborted"))
      ) {
        return;
      }
      abortRef.current = null;
      setState((prev) => ({
        ...prev,
        isFollowing: false,
        error: err instanceof Error ? err : new Error(String(err)),
      }));
    }
  }, []);

  const stop = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setState((prev) => ({
      ...prev,
      isFollowing: false,
      error: null,
    }));
  }, []);

  const reset = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setState({
      records: [],
      isFollowing: false,
      error: null,
    });
  }, []);

  return {
    ...state,
    follow,
    stop,
    reset,
  };
}
