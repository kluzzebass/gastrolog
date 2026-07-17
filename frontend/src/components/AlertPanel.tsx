import { useState } from "react";
import { useThemeClass } from "../hooks/useThemeClass";
import { AlarmPriority, AlarmState, collapseFloodAlerts } from "../api/hooks/useAlerts";
import type { NodeAlert, NodeFlood } from "../api/hooks/useAlerts";
import { useAlarmLifecycle } from "../api/hooks/useAlarmLifecycle";
import { encode } from "../api/glid";

interface AlertPanelProps {
  active: NodeAlert[];
  acked: NodeAlert[];
  cleared: NodeAlert[];
  shelved: NodeAlert[];
  floods: NodeFlood[];
  dark: boolean;
  onClose: () => void;
}

function formatTime(seconds: bigint | undefined): string {
  if (!seconds) return "—";
  const date = new Date(Number(seconds) * 1000);
  return date.toLocaleTimeString();
}

/** Priority → design-token mapping: Critical (and software faults) reuse the
 *  error token, High the warn token, Low the info token. */
function priorityColor(a: Pick<NodeAlert, "priority" | "softwareFault">): string {
  if (a.softwareFault || a.priority === AlarmPriority.CRITICAL) return "text-severity-error";
  if (a.priority === AlarmPriority.HIGH) return "text-severity-warn";
  return "text-severity-info";
}

function priorityLabel(a: Pick<NodeAlert, "priority" | "softwareFault">): string {
  if (a.softwareFault) return "fault";
  switch (a.priority) {
    case AlarmPriority.CRITICAL:
      return "critical";
    case AlarmPriority.HIGH:
      return "high";
    case AlarmPriority.LOW:
      return "low";
    default:
      return "—";
  }
}

function PriorityIcon({ alarm }: Readonly<{ alarm: NodeAlert }>) {
  const color = priorityColor(alarm);
  if (alarm.softwareFault || alarm.priority === AlarmPriority.CRITICAL) {
    return (
      <svg viewBox="0 0 16 16" className={`w-4 h-4 ${color} flex-shrink-0`} fill="currentColor">
        <circle cx="8" cy="8" r="7" />
      </svg>
    );
  }
  if (alarm.priority === AlarmPriority.HIGH) {
    return (
      <svg viewBox="0 0 16 16" className={`w-4 h-4 ${color} flex-shrink-0`} fill="currentColor">
        <path d="M8 1L15 14H1L8 1Z" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 16 16" className={`w-4 h-4 ${color} flex-shrink-0`} fill="currentColor">
      <circle cx="8" cy="8" r="4" />
    </svg>
  );
}

/** Shelve duration choices. Every shelve expires — no permanent option. */
const SHELVE_CHOICES: ReadonlyArray<{ label: string; seconds: number }> = [
  { label: "30 min", seconds: 30 * 60 },
  { label: "2 h", seconds: 2 * 3600 },
  { label: "8 h", seconds: 8 * 3600 },
  { label: "24 h", seconds: 24 * 3600 },
];

export function AlertPanel({
  active,
  acked,
  cleared,
  shelved,
  floods,
  dark,
  onClose,
}: Readonly<AlertPanelProps>) {
  const c = useThemeClass(dark);
  const lifecycle = useAlarmLifecycle();
  const [actionError, setActionError] = useState<string | null>(null);
  // Flood-mode collapse: same-type alarms of a flooding node fold into one
  // row with a count. Aggregation-side only — the wire keeps every instance.
  const floodingNodeIds = new Set(floods.map((f) => f.nodeId));
  const groups = collapseFloodAlerts(active, floodingNodeIds);
  const [expandedGroups, setExpandedGroups] = useState<ReadonlySet<string>>(new Set());
  const toggleGroup = (key: string) =>
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  const onError = (err: unknown) =>
    setActionError(err instanceof Error ? err.message : String(err));
  const handlers = {
    onAck: (a: NodeAlert) => {
      setActionError(null);
      lifecycle.ack.mutate(a.id, { onError });
    },
    onShelve: (a: NodeAlert, seconds: number) => {
      setActionError(null);
      lifecycle.shelve.mutate({ alarmId: a.id, durationSeconds: seconds }, { onError });
    },
    onUnshelve: (a: NodeAlert) => {
      setActionError(null);
      lifecycle.unshelve.mutate(a.id, { onError });
    },
  };

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-16" onClick={onClose}>
      <div
        className={`w-full max-w-lg mx-4 rounded-lg shadow-xl border ${c(
          "bg-ink-raised border-ink-border-subtle",
          "bg-light-raised border-light-border-subtle",
        )}`}
        onClick={(e) => e.stopPropagation()}
      >
        <div
          className={`flex items-center justify-between px-4 py-3 border-b ${c(
            "border-ink-border-subtle",
            "border-light-border-subtle",
          )}`}
        >
          <h2
            className={`font-display text-lg font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            System Alerts
          </h2>
          <button
            onClick={onClose}
            className={`w-7 h-7 flex items-center justify-center rounded ${c(
              "text-text-muted hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-muted hover:text-light-text-muted hover:bg-light-hover",
            )}`}
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>

        {floods.length > 0 && (
          <div
            className={`px-4 py-2 border-b bg-severity-warn/10 ${c(
              "border-ink-border-subtle",
              "border-light-border-subtle",
            )}`}
          >
            {floods.map((f) => (
              <p key={f.nodeId} className="text-xs font-mono text-severity-warn">
                Alarm flood on {f.nodeName} — {f.rate} alarms in 10 min. Same-type alarms are
                collapsed below.
              </p>
            ))}
          </div>
        )}

        {actionError && (
          <div
            className={`px-4 py-2 border-b bg-severity-error/10 ${c(
              "border-ink-border-subtle",
              "border-light-border-subtle",
            )}`}
          >
            <p className="text-xs font-mono text-severity-error">{actionError}</p>
          </div>
        )}

        <div className="max-h-96 overflow-y-auto">
          {active.length === 0 && (
            <p className={`px-4 py-3 text-sm ${c("text-text-muted", "text-light-text-muted")}`}>
              No active alarms.
            </p>
          )}
          {groups.map((g) => {
            const head = g.alerts[0]!;
            if (g.alerts.length === 1) {
              return <AlertRow key={g.key} alarm={head} dark={dark} {...handlers} />;
            }
            const expanded = expandedGroups.has(g.key);
            return (
              <div
                key={g.key}
                className={`border-b last:border-b-0 ${c(
                  "border-ink-border-subtle",
                  "border-light-border-subtle",
                )}`}
              >
                <button
                  onClick={() => toggleGroup(g.key)}
                  aria-expanded={expanded}
                  className={`w-full text-left flex gap-3 px-4 py-3 ${c(
                    "hover:bg-ink-hover",
                    "hover:bg-light-hover",
                  )}`}
                >
                  <PriorityIcon alarm={head} />
                  <div className="flex-1 min-w-0">
                    <p className={`text-sm ${c("text-text-normal", "text-light-text-normal")}`}>
                      {head.cause || head.detail}
                    </p>
                    <div
                      className={`flex gap-3 mt-1 text-xs font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                    >
                      <span className={priorityColor(head)}>{priorityLabel(head)}</span>
                      <span>{head.nodeName}</span>
                      <span>{head.typeId}</span>
                      <span>
                        ×{g.alerts.length} {expanded ? "▾" : "▸"}
                      </span>
                    </div>
                  </div>
                </button>
                {expanded &&
                  g.alerts.map((a) => (
                    <AlertRow
                      key={`${a.nodeId}:${encode(a.id)}`}
                      alarm={a}
                      dark={dark}
                      nested
                      {...handlers}
                    />
                  ))}
              </div>
            );
          })}

          <AlertSection
            title="Acknowledged"
            hint="condition standing; operator aware"
            alerts={acked}
            dark={dark}
            {...handlers}
          />
          <AlertSection
            title="Shelved"
            hint="suppressed until expiry"
            alerts={shelved}
            dark={dark}
            {...handlers}
          />
          <AlertSection
            title="Cleared, unacknowledged"
            hint="fired while you were away"
            alerts={cleared}
            dark={dark}
            {...handlers}
          />
        </div>
      </div>
    </div>
  );
}

interface RowHandlers {
  onAck: (a: NodeAlert) => void;
  onShelve: (a: NodeAlert, seconds: number) => void;
  onUnshelve: (a: NodeAlert) => void;
}

/** A collapsed lifecycle section below the active list: quiet until opened. */
function AlertSection({
  title,
  hint,
  alerts,
  dark,
  ...handlers
}: Readonly<{ title: string; hint: string; alerts: NodeAlert[]; dark: boolean } & RowHandlers>) {
  const c = useThemeClass(dark);
  const [open, setOpen] = useState(false);
  if (alerts.length === 0) return null;
  return (
    <div className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}>
      <button
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        className={`w-full text-left flex items-baseline gap-2 px-4 py-2 text-xs font-mono ${c(
          "text-text-muted hover:bg-ink-hover",
          "text-light-text-muted hover:bg-light-hover",
        )}`}
      >
        <span>{open ? "▾" : "▸"}</span>
        <span className={c("text-text-normal", "text-light-text-normal")}>{title}</span>
        <span>×{alerts.length}</span>
        <span className="ml-auto">{hint}</span>
      </button>
      {open &&
        alerts.map((a) => (
          <AlertRow key={`${a.nodeId}:${encode(a.id)}`} alarm={a} dark={dark} {...handlers} />
        ))}
    </div>
  );
}

function AlertRow({
  alarm: a,
  dark,
  nested,
  onAck,
  onShelve,
  onUnshelve,
}: Readonly<{ alarm: NodeAlert; dark: boolean; nested?: boolean } & RowHandlers>) {
  const c = useThemeClass(dark);
  const [expanded, setExpanded] = useState(false);
  const isShelved = a.state === AlarmState.SHELVED;
  const isAcked = a.state === AlarmState.ACTIVE_ACKED;
  // No shelve control at all for unshelveable types or non-standing states —
  // an enabled control that errors on click is worse than no control.
  const canShelve = a.shelveable && !isShelved && a.state !== AlarmState.CLEARED_UNACKED;
  const canAck = !isAcked && !isShelved;
  const actionClass = c(
    "text-text-muted hover:text-text-bright",
    "text-light-text-muted hover:text-light-text-bright",
  );
  return (
    <div
      className={`flex gap-3 py-3 ${nested ? "pl-11 pr-4" : "px-4 border-b last:border-b-0"} ${c(
        "border-ink-border-subtle",
        "border-light-border-subtle",
      )}`}
    >
      <PriorityIcon alarm={a} />
      <div className="flex-1 min-w-0">
        <button
          onClick={() => setExpanded((v) => !v)}
          aria-expanded={expanded}
          className="w-full text-left"
          title={expanded ? "Hide cause and response" : "Show cause and response"}
        >
          <p className={`text-sm ${c("text-text-normal", "text-light-text-normal")}`}>{a.detail}</p>
        </button>
        {expanded && a.cause && (
          <p className={`mt-1 text-xs ${c("text-text-muted", "text-light-text-muted")}`}>
            {a.cause}
          </p>
        )}
        {a.response && (
          <p className={`mt-1 text-xs ${c("text-text-muted", "text-light-text-muted")}`}>
            {a.response}
          </p>
        )}
        <div className={`flex flex-wrap items-baseline gap-3 mt-1 text-xs font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
          <span className={priorityColor(a)}>{priorityLabel(a)}</span>
          <span>{a.nodeName}</span>
          <span>{a.source}</span>
          <span title="First seen">{formatTime(a.firstSeen?.seconds)}</span>
          {a.occurrences > 1 && <span title="Occurrences">×{a.occurrences}</span>}
          {isAcked && (
            <span title={`Acknowledged by ${a.ackedBy}`}>ack {a.ackedBy}</span>
          )}
          {isShelved && (
            <span title="Shelved until">until {formatTime(a.shelvedUntil?.seconds)}</span>
          )}
          <span className="ml-auto flex items-baseline gap-2">
            {canAck && (
              <button
                onClick={() => onAck(a)}
                className={actionClass}
                title="Acknowledge — records your awareness; a cleared alarm is released"
              >
                ack
              </button>
            )}
            {canShelve && (
              <select
                value=""
                onChange={(e) => {
                  const secs = Number(e.target.value);
                  if (secs > 0) onShelve(a, secs);
                }}
                className={`bg-transparent border-none p-0 text-xs font-mono cursor-pointer ${actionClass}`}
                title="Shelve — suppress for a duration; always expires"
              >
                <option value="">shelve…</option>
                {SHELVE_CHOICES.map((s) => (
                  <option key={s.seconds} value={s.seconds}>
                    {s.label}
                  </option>
                ))}
              </select>
            )}
            {isShelved && (
              <button onClick={() => onUnshelve(a)} className={actionClass} title="End the shelve now">
                unshelve
              </button>
            )}
          </span>
        </div>
      </div>
    </div>
  );
}
