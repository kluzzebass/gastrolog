import { useQuery } from "@tanstack/react-query";
import { lifecycleClient } from "../client";
import { ListEventsResponse, SystemEvent } from "../gen/gastrolog/v1/lifecycle_pb";
import { protoSharing } from "./protoSharing";

// Components consume proto entities through hooks, never src/api/gen/
// directly (gastrolog-2e2qs) — the event entity is re-exported here.
export type { SystemEvent } from "../gen/gastrolog/v1/lifecycle_pb";

// Event journal (gastrolog-1m3e0d). ListEvents is a cluster-wide merge:
// the serving node reads its own in-memory ring and fans out to every peer,
// so this single call sees every node's journal. The UI fetches the merged
// list unfiltered (the server caps it at its default limit, newest kept)
// and filters client-side — that keeps both filter dropdowns populated
// from the full picture instead of collapsing to the current selection.
export function useEvents() {
  return useQuery({
    queryKey: ["events"],
    queryFn: async () => await lifecycleClient.listEvents({}),
    structuralSharing: protoSharing(ListEventsResponse.equals),
    // Events are history, not live state: refresh when the page is
    // (re)opened or refocused rather than by polling.
    staleTime: 15_000,
  });
}

export interface EventFilters {
  type: string; // "" = all
  source: string; // "" = all
}

// filterEvents applies the type/source dropdowns client-side.
export function filterEvents(events: SystemEvent[], filters: EventFilters): SystemEvent[] {
  return events.filter(
    (e) =>
      (filters.type === "" || e.type === filters.type) &&
      (filters.source === "" || e.source === filters.source),
  );
}

// eventFilterOptions derives the dropdown option lists from the full
// (unfiltered) event list, sorted and deduplicated.
export function eventFilterOptions(events: SystemEvent[]): { types: string[]; sources: string[] } {
  const types = new Set<string>();
  const sources = new Set<string>();
  for (const e of events) {
    if (e.type) types.add(e.type);
    if (e.source) sources.add(e.source);
  }
  return {
    types: [...types].sort((a, b) => a.localeCompare(b)),
    sources: [...sources].sort((a, b) => a.localeCompare(b)),
  };
}

// eventNodeLabel resolves the display name for the journaling node:
// resolved config name first, node ID as fallback — attribution is never
// blank.
export function eventNodeLabel(e: SystemEvent): string {
  if (e.nodeName !== "") return e.nodeName;
  return new TextDecoder().decode(e.nodeId);
}
