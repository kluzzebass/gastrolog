import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import {
  LogLevel,
  type LogLevelConfig,
} from "../gen/gastrolog/v1/system_pb";
import { useQuery } from "@tanstack/react-query";

/**
 * usePutLogLevels mutates the cluster-wide LogLevelConfig (gastrolog-3flfp).
 * The mutation replaces the entire rule set in one Raft commit; callers
 * who only want to add/remove individual rules should read the current
 * config first (via useConfig), modify locally, then dispatch.
 */
export function usePutLogLevels() {
  return useSystemMutation(
    async (config: LogLevelConfig) => {
      return systemClient.putLogLevels({ config });
    },
    // After a rule swap, every component path's effective level may
    // have changed; invalidate the components query so the reference
    // table re-fetches with new resolutions.
    [["log-components"]],
  );
}

/**
 * useLogComponents enumerates every component path the binary's comp
 * registry has built, with each path's currently-effective level and
 * resolution source.
 *
 * The list is static per binary version (paths are declared at startup);
 * what changes is the effective level when LogLevelConfig is updated.
 * The query invalidates with the system key so a fresh PutLogLevels
 * triggers a refetch.
 */
export function useLogComponents() {
  return useQuery({
    queryKey: ["log-components"],
    queryFn: async () => {
      const resp = await systemClient.listLogComponents({});
      return resp.components;
    },
    staleTime: 30_000,
  });
}

/** levelLabel maps the LogLevel enum to operator-friendly text. */
export function levelLabel(l: LogLevel): string {
  switch (l) {
    case LogLevel.DEBUG: return "debug";
    case LogLevel.INFO: return "info";
    case LogLevel.WARN: return "warn";
    case LogLevel.ERROR: return "error";
    default: return "";
  }
}
