import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import {
  useEvents,
  filterEvents,
  eventFilterOptions,
  eventNodeLabel,
  type EventFilters,
  type SystemEvent,
} from "../../api/hooks/useEvents";
import { protoToInstant, formatTimestamp } from "../../utils/temporal";

// Event journal page (gastrolog-1m3e0d). Deliberately QUIETER than the
// alarm list: events are records of occurrence, not calls to action, so
// there are no ack/shelve controls and no severity coloring — the three
// text levels carry the whole hierarchy. Filterable by type and source;
// rendered newest first.
//
// Restart semantics, made visible: journals are per-node and in-memory.
// Each node's history begins with a node-started entry at its boot
// instant, and a node the serving node could not reach is named — silence
// from it is unknown state, never quiet history.
export function EventJournalView({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useEvents();
  const [filters, setFilters] = useState<EventFilters>({ type: "", source: "" });

  if (isLoading || !data) {
    return <LoadingPlaceholder dark={dark} />;
  }

  const all = data.events;
  const { types, sources } = eventFilterOptions(all);
  const shown = filterEvents(all, filters).toReversed(); // newest first
  const unreachable = data.unreachableNodes;

  const selectClass = `bg-transparent border rounded px-2 py-1 text-[0.8em] font-mono cursor-pointer ${c(
    "border-ink-border text-text-normal",
    "border-light-border text-light-text-normal",
  )}`;

  return (
    <div className="flex flex-col gap-3">
      {/* Filters — quiet selects, no color */}
      <div className="flex items-center gap-2">
        <select
          value={filters.type}
          onChange={(e) => setFilters({ ...filters, type: e.target.value })}
          className={selectClass}
          aria-label="Filter by event type"
        >
          <option value="">all types</option>
          {types.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
        <select
          value={filters.source}
          onChange={(e) => setFilters({ ...filters, source: e.target.value })}
          className={selectClass}
          aria-label="Filter by source"
        >
          <option value="">all sources</option>
          {sources.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <span className={`ml-auto text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
          {shown.length} of {all.length}
        </span>
      </div>

      {/* Unreachable nodes: their journals are missing, not empty. */}
      {unreachable.length > 0 && (
        <div className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
          No journal from {unreachable.join(", ")} — events from there are missing, not absent.
        </div>
      )}

      {shown.length === 0 ? (
        <div className={`text-[0.9em] py-8 text-center ${c("text-text-muted", "text-light-text-muted")}`}>
          No events match.
        </div>
      ) : (
        <div className={`rounded-lg border overflow-hidden ${c("border-ink-border", "border-light-border")}`}>
          <div
            className={`grid grid-cols-[9.5rem_7rem_9.5rem_1fr] gap-3 px-4 py-2 text-[0.7em] font-medium uppercase tracking-[0.15em] border-b ${c(
              "text-text-muted border-ink-border-subtle bg-ink-well",
              "text-light-text-muted border-light-border-subtle bg-light-well",
            )}`}
          >
            <span>Time</span>
            <span>Node</span>
            <span>Event</span>
            <span>Detail</span>
          </div>
          {shown.map((e) => (
            <EventRow key={`${eventNodeLabel(e)}-${e.seq}`} event={e} dark={dark} />
          ))}
        </div>
      )}

      {/* The restart decision, stated where the data is read. */}
      <div className={`text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}>
        Per-node in-memory journal — each node's history begins at its node-started entry and does
        not survive restart.
      </div>
    </div>
  );
}

function EventRow({ event, dark }: Readonly<{ event: SystemEvent; dark: boolean }>) {
  const c = useThemeClass(dark);
  const time = event.time ? formatTimestamp(protoToInstant(event.time)) : "";
  const alarmId = new TextDecoder().decode(event.alarmId);
  return (
    <div
      className={`grid grid-cols-[9.5rem_7rem_9.5rem_1fr] gap-3 px-4 py-2 text-[0.8em] border-b last:border-b-0 ${c(
        "border-ink-border-subtle",
        "border-light-border-subtle",
      )}`}
    >
      <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>{time}</span>
      <span className={`truncate ${c("text-text-normal", "text-light-text-normal")}`} title={eventNodeLabel(event)}>
        {eventNodeLabel(event)}
      </span>
      <span className={`font-mono ${c("text-text-normal", "text-light-text-normal")}`}>{event.type}</span>
      <span className={c("text-text-muted", "text-light-text-muted")}>
        {alarmId !== "" && (
          <span className={`font-mono ${c("text-text-bright", "text-light-text-bright")}`}>{alarmId}: </span>
        )}
        {event.detail}
        {event.by !== "" && <span> (by {event.by})</span>}
        {event.source !== "" && (
          <span className={`ml-1 font-mono text-[0.9em] ${c("text-text-muted", "text-light-text-muted")}`}>
            [{event.source}]
          </span>
        )}
      </span>
    </div>
  );
}
