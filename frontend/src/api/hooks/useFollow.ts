import { useState, useCallback, useRef } from "react";
import { queryClient, Query, Record } from "../client";

const MAX_RECORDS = 5000;
const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30_000;

export interface FollowState {
  records: Record[];
  isFollowing: boolean;
  reconnecting: boolean;
  reconnectAttempt: number;
  error: Error | null;
}

function isAbortError(err: unknown): boolean {
  return (
    err instanceof Error &&
    (err.name === "AbortError" || err.message.includes("aborted"))
  );
}

export function useFollow() {
  const [state, setState] = useState<FollowState>({
    records: [],
    isFollowing: false,
    reconnecting: false,
    reconnectAttempt: 0,
    error: null,
  });

  const abortRef = useRef<AbortController | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Ref to the current query string so reconnect can re-use it.
  const queryRef = useRef<string>("");
  // Ref to the record buffer so reconnects preserve existing records.
  const bufferRef = useRef<Record[]>([]);

  const cancelReconnect = useCallback(() => {
    if (reconnectTimer.current !== null) {
      clearTimeout(reconnectTimer.current);
      reconnectTimer.current = null;
    }
  }, []);

  const connectStream = useCallback(
    async (queryStr: string, attempt: number) => {
      // Cancel any previous stream.
      if (abortRef.current) {
        abortRef.current.abort();
      }
      abortRef.current = new AbortController();

      const stripped = queryStr
        .replace(/\bstart=\S+/g, "")
        .replace(/\bend=\S+/g, "")
        .replace(/\s+/g, " ")
        .trim();

      const query = new Query();
      query.expression = stripped;

      try {
        const buffer = bufferRef.current;

        for await (const response of queryClient.follow(
          { query },
          { signal: abortRef.current.signal },
        )) {
          // Connection is live — reset reconnect state on first message.
          if (attempt > 0) {
            setState((prev) => ({
              ...prev,
              reconnecting: false,
              reconnectAttempt: 0,
              error: null,
            }));
            attempt = 0;
          }

          buffer.unshift(...response.records);
          if (buffer.length > MAX_RECORDS) {
            buffer.length = MAX_RECORDS;
          }

          setState((prev) => ({
            ...prev,
            records: [...buffer],
          }));
        }

        // Stream ended unexpectedly — schedule reconnect.
        scheduleReconnect(queryStr, 0);
      } catch (err) {
        if (isAbortError(err)) {
          return;
        }
        // Schedule reconnect with backoff.
        scheduleReconnect(queryStr, attempt);
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  const scheduleReconnect = useCallback(
    (queryStr: string, attempt: number) => {
      const nextAttempt = attempt + 1;
      const delay = Math.min(INITIAL_BACKOFF_MS * 2 ** attempt, MAX_BACKOFF_MS);

      abortRef.current = null;
      setState((prev) => ({
        ...prev,
        reconnecting: true,
        reconnectAttempt: nextAttempt,
        error: null,
      }));

      reconnectTimer.current = setTimeout(() => {
        reconnectTimer.current = null;
        connectStream(queryStr, nextAttempt);
      }, delay);
    },
    [connectStream],
  );

  const follow = useCallback(
    async (queryStr: string) => {
      cancelReconnect();
      queryRef.current = queryStr;
      bufferRef.current = [];

      setState({
        records: [],
        isFollowing: true,
        reconnecting: false,
        reconnectAttempt: 0,
        error: null,
      });

      connectStream(queryStr, 0);
    },
    [connectStream, cancelReconnect],
  );

  const stop = useCallback(() => {
    cancelReconnect();
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    setState((prev) => ({
      ...prev,
      isFollowing: false,
      reconnecting: false,
      reconnectAttempt: 0,
      error: null,
    }));
  }, [cancelReconnect]);

  const reset = useCallback(() => {
    cancelReconnect();
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    bufferRef.current = [];
    setState({
      records: [],
      isFollowing: false,
      reconnecting: false,
      reconnectAttempt: 0,
      error: null,
    });
  }, [cancelReconnect]);

  return {
    ...state,
    follow,
    stop,
    reset,
  };
}
