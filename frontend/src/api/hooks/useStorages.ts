import { useQuery } from "@tanstack/react-query";
import { systemClient } from "../client";
import { StorageState } from "../gen/gastrolog/v1/storage_pb";
import { protoArraySharing } from "./protoSharing";
import { Storage } from "../model/storage";

/**
 * Returns every configured storage cluster-wide as Storage models — the
 * entity-list surface for the storage inspector. Mirrors useVaults:
 * ListStorages already composes identity (config) with live guard state
 * (local-or-peer) server-side, so there is no client-side join here, just
 * the raw-bytes → EntityID reshape the Storage wrapper does.
 *
 * Push-invalidated by WatchSystemStatus's `storages` field (see
 * useWatchSystemStatus), which writes straight into this same query key —
 * staleTime is a polling safety net only.
 */
export function useStorages(): { data: Storage[]; isLoading: boolean } {
  const { data, isLoading } = useQuery({
    queryKey: ["storages"],
    queryFn: async () => {
      const response = await systemClient.listStorages({});
      return response.storages;
    },
    structuralSharing: protoArraySharing(StorageState.equals),
    staleTime: 60_000,
  });

  return { data: (data ?? []).map((s) => new Storage(s)), isLoading };
}
