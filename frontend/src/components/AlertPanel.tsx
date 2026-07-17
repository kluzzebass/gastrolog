import { useThemeClass } from "../hooks/useThemeClass";
import { AlarmPriority } from "../api/hooks/useAlerts";
import type { NodeAlert } from "../api/hooks/useAlerts";
import { encode } from "../api/glid";

interface AlertPanelProps {
  alerts: NodeAlert[];
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

export function AlertPanel({ alerts, dark, onClose }: Readonly<AlertPanelProps>) {
  const c = useThemeClass(dark);

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

        <div className="max-h-80 overflow-y-auto">
          {alerts.map((a) => (
            <div
              key={`${a.nodeId}:${encode(a.id)}`}
              className={`flex gap-3 px-4 py-3 border-b last:border-b-0 ${c(
                "border-ink-border-subtle",
                "border-light-border-subtle",
              )}`}
            >
              <PriorityIcon alarm={a} />
              <div className="flex-1 min-w-0">
                <p className={`text-sm ${c("text-text-normal", "text-light-text-normal")}`}>
                  {a.detail}
                </p>
                {a.response && (
                  <p className={`mt-1 text-xs ${c("text-text-muted", "text-light-text-muted")}`}>
                    {a.response}
                  </p>
                )}
                <div className={`flex gap-3 mt-1 text-xs font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
                  <span className={priorityColor(a)}>{priorityLabel(a)}</span>
                  <span>{a.nodeName}</span>
                  <span>{a.source}</span>
                  <span title="First seen">{formatTime(a.firstSeen?.seconds)}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
