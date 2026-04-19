import { useWatchSystem } from "./api/hooks/useWatchSystem";
import { useWatchChunks } from "./api/hooks/useWatchChunks";
import { useWatchSystemStatus } from "./api/hooks/useWatchSystemStatus";

/**
 * Long-lived Connect streams must not live under React.StrictMode: in dev,
 * StrictMode mounts → unmounts → remounts children, which would start duplicate
 * streaming fetches (WatchSystem, WatchChunks, WatchSystemStatus) and can
 * exhaust the browser/proxy connection budget so unary RPCs stall as (pending).
 */
export function WatchStreamsHost() {
  useWatchSystem();
  useWatchChunks();
  useWatchSystemStatus();
  return null;
}
