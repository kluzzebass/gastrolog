import { useState, useCallback, useRef, useEffect, type MutableRefObject } from "react";
import { useQuery } from "@tanstack/react-query";
import { ConnectError, Code } from "@connectrpc/connect";
import { jobClient } from "../client";
import { JobStatus } from "../gen/gastrolog/v1/job_pb";
import type { Job } from "../gen/gastrolog/v1/job_pb";

const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30_000;

function isAbortError(err: unknown): boolean {
  return (
    err instanceof Error &&
    (err.name === "AbortError" || err.message.includes("aborted"))
  );
}

export interface WatchJobsState {
  jobs: Job[];
  connected: boolean;
  reconnecting: boolean;
  error: Error | null;
}

export function useWatchJobs(options?: { onError?: (err: Error) => void }) {
  const onErrorRef = useRef(options?.onError) as MutableRefObject<((err: Error) => void) | undefined>;
  onErrorRef.current = options?.onError;

  const [state, setState] = useState<WatchJobsState>({
    jobs: [],
    connected: false,
    reconnecting: false,
    error: null,
  });

  const abortRef = useRef<AbortController | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const flushRef = useRef<number | null>(null);
  const dirtyRef = useRef(false);
  const jobsRef = useRef<Job[]>([]);

  const cancelReconnect = useCallback(() => {
    if (reconnectTimer.current !== null) {
      clearTimeout(reconnectTimer.current);
      reconnectTimer.current = null;
    }
  }, []);

  const connectStream = useCallback(
    async (attempt: number) => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
      abortRef.current = new AbortController();

      try {
        for await (const response of jobClient.watchJobs(
          {},
          { signal: abortRef.current.signal },
        )) {
          if (attempt > 0) {
            attempt = 0;
          }

          jobsRef.current = response.jobs;
          dirtyRef.current = true;

          if (flushRef.current === null) {
            flushRef.current = requestAnimationFrame(() => {
              flushRef.current = null;
              if (dirtyRef.current) {
                dirtyRef.current = false;
                setState({
                  jobs: [...jobsRef.current],
                  connected: true,
                  reconnecting: false,
                  error: null,
                });
              }
            });
          }
        }

        // Stream ended — schedule reconnect.
        scheduleReconnect(0);
      } catch (err) {
        if (isAbortError(err)) {
          return;
        }
        if (
          err instanceof ConnectError &&
          err.code === Code.Unauthenticated
        ) {
          setState((prev) => ({
            ...prev,
            error: err as ConnectError,
            connected: false,
            reconnecting: false,
          }));
          return;
        }
        onErrorRef.current?.(err instanceof Error ? err : new Error(String(err)));
        scheduleReconnect(attempt);
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  const scheduleReconnect = useCallback(
    (attempt: number) => {
      const nextAttempt = attempt + 1;
      const delay = Math.min(
        INITIAL_BACKOFF_MS * 2 ** attempt,
        MAX_BACKOFF_MS,
      );

      abortRef.current = null;
      setState((prev) => ({
        ...prev,
        connected: false,
        reconnecting: true,
        error: null,
      }));

      reconnectTimer.current = setTimeout(() => {
        reconnectTimer.current = null;
        connectStream(nextAttempt);
      }, delay);
    },
    [connectStream],
  );

  useEffect(() => {
    connectStream(0);
    return () => {
      cancelReconnect();
      if (flushRef.current !== null) {
        cancelAnimationFrame(flushRef.current);
        flushRef.current = null;
      }
      if (abortRef.current) {
        abortRef.current.abort();
        abortRef.current = null;
      }
    };
  }, [connectStream, cancelReconnect]);

  return state;
}

export function useJob(jobId: string | null) {
  return useQuery({
    queryKey: ["job", jobId],
    queryFn: async () => {
      const response = await jobClient.getJob({ id: jobId! });
      return response.job;
    },
    enabled: !!jobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status === JobStatus.COMPLETED || status === JobStatus.FAILED)
        return false;
      return 1000;
    },
  });
}
