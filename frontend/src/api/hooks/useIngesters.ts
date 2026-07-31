import { useState, useEffect, useRef } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { ConnectError, Code } from "@connectrpc/connect";
import { systemClient } from "../client";
import { WatchIngesterStatusResponse } from "../gen/gastrolog/v1/system_pb";
import { useSystemMutation, useConfig } from "./useSystem";
import { decode, encodeString } from "../glid";
import { Ingester } from "../model/ingester";

/**
 * Returns the cluster's configured ingesters as model instances. Derived
 * from the cached `GetSystem` config — no extra RPC, no separate cache key.
 * Runtime overlay (per-node alive map) is queried separately via
 * `useIngesterAlive`; the model's methods accept it as an argument so
 * callers stay in control of when they need it.
 */
export function useIngesters(): Ingester[] {
  const { data: config } = useConfig();
  return (config?.ingesters ?? []).map((c) => new Ingester(c));
}

// useIngesterStatus returns a live view of a single ingester's status.
// Event-driven via the WatchIngesterStatus server stream — no polling.
// Shape matches the previous useQuery-based hook ({ data, isLoading }) so
// consumers don't need to change.
export function useIngesterStatus(id: string) {
  const [data, setData] = useState<WatchIngesterStatusResponse | undefined>(undefined);
  const [isLoading, setIsLoading] = useState(!!id);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!id) {
      setData(undefined);
      setIsLoading(false);
      return;
    }
    setIsLoading(true);
    const ac = new AbortController();
    abortRef.current = ac;

    (async () => {
      try {
        for await (const response of systemClient.watchIngesterStatus(
          { id: decode(id) },
          { signal: ac.signal },
        )) {
          setData(response);
          setIsLoading(false);
        }
      } catch (err) {
        if (ac.signal.aborted) return;
        // NotFound / Unimplemented just stops the stream; consumer handles
        // undefined data.
        if (err instanceof ConnectError && (err.code === Code.NotFound || err.code === Code.Unimplemented)) {
          setIsLoading(false);
          return;
        }
        // Other errors: keep the last-known data but stop loading state.
        setIsLoading(false);
      }
    })();

    return () => {
      ac.abort();
    };
  }, [id]);

  return { data, isLoading };
}

/** Trim whitespace and strip empty values so the backend treats them as unset. */
function stripEmptyParams(params: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params)) {
    const trimmed = v.trim();
    if (trimmed !== "") out[k] = trimmed;
  }
  return out;
}

export function usePutIngester() {
  return useSystemMutation(
    async (args: {
      id: string;
      name: string;
      type: string;
      enabled: boolean;
      params: Record<string, string>;
      nodeIds?: string[];
      allNodes?: boolean;
      singleton?: boolean;
    }) => {
      return systemClient.putIngester({
        config: {
          id: decode(args.id),
          name: args.name,
          type: args.type,
          enabled: args.enabled,
          params: stripEmptyParams(args.params),
          nodeIds: (args.nodeIds ?? []).map(encodeString),
          allNodes: args.allNodes ?? false,
          singleton: args.singleton ?? false,
        },
      });
    },
  );
}

export function useDeleteIngester() {
  return useSystemMutation(
    async (id: string) => {
      return systemClient.deleteIngester({ id: decode(id) });
    },
  );
}

export function useTestIngester() {
  return useMutation({
    mutationFn: async (args: { type: string; params: Record<string, string>; id?: string }) => {
      const response = await systemClient.testIngester({
        type: args.type,
        params: stripEmptyParams(args.params),
        id: args.id ? decode(args.id) : new Uint8Array(16),
      });
      return response;
    },
  });
}

/**
 * Auto-checks listen address availability for listener ingesters.
 * Calls TestIngester on the server with debounce whenever params change.
 *
 * The assignment is part of the question, not context: a port is free or taken
 * per node, so the server has to know which nodes to ask. Omitting it asks only
 * whichever node served the request, which is the whole node it happens to be
 * connected to and no others.
 *
 * The assignment is also part of the query key — changing which nodes an
 * ingester targets changes the answer, so a cached verdict for the previous
 * assignment must not be reused.
 */
export function useCheckListenAddrs(
  type: string,
  params: Record<string, string>,
  id: string,
  nodeIds: string[] = [],
  allNodes = false,
) {
  const LISTENER_TYPES = new Set(["syslog", "http", "fluentfwd", "otlp", "relp"]);
  const isListener = LISTENER_TYPES.has(type);

  // Build a stable key from the address-relevant params.
  const stripped = stripEmptyParams(params);
  const paramKey = isListener ? JSON.stringify(stripped) : "";
  const assignmentKey = allNodes ? "all" : [...nodeIds].sort().join(",");

  return useQuery({
    queryKey: ["checkListenAddrs", type, paramKey, id, assignmentKey],
    queryFn: async () => {
      const response = await systemClient.testIngester({
        type,
        params: stripped,
        id: id ? decode(id) : new Uint8Array(16),
        nodeIds: nodeIds.map(decode),
        allNodes,
      });
      return response;
    },
    enabled: isListener && paramKey !== "{}",
    staleTime: 5_000,
    gcTime: 10_000,
  });
}
