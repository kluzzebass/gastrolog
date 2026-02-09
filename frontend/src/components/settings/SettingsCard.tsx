import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";

interface SettingsCardProps {
  id: string;
  typeBadge?: string;
  dark: boolean;
  expanded?: boolean;
  onToggle?: () => void;
  onDelete?: () => void;
  deleteLabel?: string;
  children: React.ReactNode;
  status?: React.ReactNode;
}

export function SettingsCard({
  id,
  typeBadge,
  dark,
  expanded,
  onToggle,
  onDelete,
  deleteLabel,
  children,
  status,
}: SettingsCardProps) {
  const c = useThemeClass(dark);
  const [confirmDelete, setConfirmDelete] = useState(false);

  return (
    <div
      className={`border rounded-lg overflow-hidden transition-colors ${c(
        "border-ink-border-subtle bg-ink-surface",
        "border-light-border-subtle bg-light-surface",
      )}`}
    >
      {/* Header */}
      <div
        className={`flex items-center justify-between px-4 py-3 cursor-pointer select-none transition-colors ${c(
          "hover:bg-ink-hover",
          "hover:bg-light-hover",
        )}`}
        onClick={onToggle}
        {...clickableProps(onToggle)}
        aria-expanded={expanded}
      >
        <div className="flex items-center gap-2.5">
          <span
            className={`text-[0.7em] transition-transform ${expanded ? "rotate-90" : ""}`}
          >
            {"\u25B6"}
          </span>
          <span
            className={`font-mono text-[0.9em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}
          >
            {id}
          </span>
          {typeBadge && (
            <span
              className={`px-1.5 py-0.5 text-[0.8em] font-mono rounded ${c(
                "bg-ink-hover text-text-muted",
                "bg-light-hover text-light-text-muted",
              )}`}
            >
              {typeBadge}
            </span>
          )}
          {status}
        </div>
        <div className="flex items-center gap-2">
          {onDelete && !confirmDelete && (
            <button
              onClick={(e) => {
                e.stopPropagation();
                setConfirmDelete(true);
              }}
              className={`px-2 py-1 text-[0.75em] rounded transition-colors ${c(
                "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
                "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
              )}`}
            >
              {deleteLabel || "Delete"}
            </button>
          )}
          {onDelete && confirmDelete && (
            <div
              className="flex items-center gap-1.5"
              onClick={(e) => e.stopPropagation()}
            >
              <span
                className={`text-[0.75em] ${c("text-severity-error", "text-severity-error")}`}
              >
                Confirm?
              </span>
              <button
                onClick={() => {
                  onDelete();
                  setConfirmDelete(false);
                }}
                className="px-2 py-1 text-[0.75em] rounded bg-severity-error/15 text-severity-error hover:bg-severity-error/25 transition-colors"
              >
                Yes
              </button>
              <button
                onClick={() => setConfirmDelete(false)}
                className={`px-2 py-1 text-[0.75em] rounded transition-colors ${c(
                  "text-text-muted hover:bg-ink-hover",
                  "text-light-text-muted hover:bg-light-hover",
                )}`}
              >
                No
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Body */}
      {expanded && (
        <div
          className={`px-4 pb-4 pt-1 border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
        >
          {children}
        </div>
      )}
    </div>
  );
}
