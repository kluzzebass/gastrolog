import { useState, useCallback, useRef } from "react";
import { ConnectError, Code } from "@connectrpc/connect";
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
  newCount: number;
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
    newCount: 0,
  });
  const newCountRef = useRef(0);

  const abortRef = useRef<AbortController | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Ref to the current query string so reconnect can re-use it.
  const queryRef = useRef<string>("");
  // Ref to the record buffer so reconnects preserve existing records.
  const bufferRef = useRef<Record[]>([]);
  // Throttle: coalesce rapid stream messages into a single render per frame.
  const flushRef = useRef<number | null>(null);
  const dirtyRef = useRef(false);

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

          newCountRef.current += response.records.length;
          dirtyRef.current = true;
          if (flushRef.current === null) {
            flushRef.current = requestAnimationFrame(() => {
              flushRef.current = null;
              if (dirtyRef.current) {
                dirtyRef.current = false;
                setState((prev) => ({
                  ...prev,
                  records: [...buffer],
                  newCount: newCountRef.current,
                }));
              }
            });
          }
        }

        // Stream ended unexpectedly — schedule reconnect.
        scheduleReconnect(queryStr, 0);
      } catch (err) {
        if (isAbortError(err)) {
          return;
        }
        // Don't reconnect on auth errors — the global interceptor will
        // redirect to login; we just need to stop the reconnect loop.
        if (
          err instanceof ConnectError &&
          err.code === Code.Unauthenticated
        ) {
          setState((prev) => ({
            ...prev,
            error: err as ConnectError,
            reconnecting: false,
          }));
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
      newCountRef.current = 0;

      setState({
        records: [],
        isFollowing: true,
        reconnecting: false,
        reconnectAttempt: 0,
        error: null,
        newCount: 0,
      });

      connectStream(queryStr, 0);
    },
    [connectStream, cancelReconnect],
  );

  const cancelFlush = useCallback(() => {
    if (flushRef.current !== null) {
      cancelAnimationFrame(flushRef.current);
      flushRef.current = null;
    }
    dirtyRef.current = false;
  }, []);

  const stop = useCallback(() => {
    cancelReconnect();
    cancelFlush();
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
  }, [cancelReconnect, cancelFlush]);

  const reset = useCallback(() => {
    cancelReconnect();
    cancelFlush();
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
    }
    bufferRef.current = [];
    newCountRef.current = 0;
    setState({
      records: [],
      isFollowing: false,
      reconnecting: false,
      reconnectAttempt: 0,
      error: null,
      newCount: 0,
    });
  }, [cancelReconnect]);

  const resetNewCount = useCallback(() => {
    newCountRef.current = 0;
    setState((prev) => ({ ...prev, newCount: 0 }));
  }, []);

  return {
    ...state,
    follow,
    stop,
    reset,
    resetNewCount,
  };
}
