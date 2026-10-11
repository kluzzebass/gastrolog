import { IngesterMode } from "../../api/gen/gastrolog/v1/system_pb";

const SINGLETON_LABEL = "Singleton (run on one node with automatic failover)";

/** The singleton checkbox's label, carrying the reason when the type cannot use it. */
export function singletonLabel(singletonSupported: boolean): string {
  return singletonSupported
    ? SINGLETON_LABEL
    : `${SINGLETON_LABEL} — not available for this type: it reads a node-local source or binds a port on every assigned node`;
}

/** The mode badge: a listener binds a port and waits for clients; an active ingester pulls from its source. */
export function ingesterModeBadge(mode: IngesterMode): { text: string; title: string } {
  return mode === IngesterMode.PASSIVE
    ? { text: "listener", title: "listener — binds a port and accepts what clients push to it" }
    : { text: "active", title: "active — pulls records from its upstream source itself" };
}
