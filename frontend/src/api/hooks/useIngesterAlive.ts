import { useQuery } from "@tanstack/react-query";
import { encode } from "../glid";
import type { IngesterAlive } from "../gen/gastrolog/v1/system_pb";

/**
 * Per-ingester alive map keyed by encoded ingester ID. Populated by the
 * WatchSystemStatus server stream (see useWatchSystemStatus) — no polling
 * and no separate ListIngesters round-trip; the alive map and the static
 * config live on the same push surface, so inspector reads stay coherent
 * with whatever the cluster's FSM holds.
 */
export type IngesterAliveMap = Map<string, { [nodeID: string]: boolean }>;

export function useIngesterAlive(): IngesterAliveMap {
  const { data } = useQuery<IngesterAliveMap>({
    queryKey: ["ingester-alive"],
    queryFn: () => new Map(),
    // The WatchSystemStatus stream writes this cache directly; we never
    // need to fetch it.
    enabled: false,
    staleTime: Infinity,
    initialData: () => new Map(),
  });
  return data;
}

/**
 * Translate a repeated IngesterAlive proto field into the cache map shape.
 * Lives here so useWatchSystemStatus can stay schema-agnostic.
 */
export function ingesterAliveListToMap(list: IngesterAlive[]): IngesterAliveMap {
  const m: IngesterAliveMap = new Map();
  for (const ia of list) {
    m.set(encode(ia.id), ia.nodeStatus);
  }
  return m;
}
