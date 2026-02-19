import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { ExpandableCard } from "./ExpandableCard";

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
  headerRight?: React.ReactNode;
  footer?: React.ReactNode;
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
  headerRight,
  footer,
}: Readonly<SettingsCardProps>) {
  const c = useThemeClass(dark);
  const [confirmDelete, setConfirmDelete] = useState(false);

  return (
    <ExpandableCard
      id={id}
      typeBadge={typeBadge}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      status={status}
      headerRight={headerRight}
    >
      {children}
      {(onDelete || footer) && (
        <div className="flex items-center justify-between pt-3 mt-3">
          <div>
            {onDelete && !confirmDelete && (
              <button
                onClick={() => setConfirmDelete(true)}
                className={`px-3 py-1.5 text-[0.8em] rounded transition-colors ${c(
                  "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
                  "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
                )}`}
              >
                {deleteLabel || "Delete"}
              </button>
            )}
            {onDelete && confirmDelete && (
              <div className="flex items-center gap-1.5">
                <span className="text-[0.8em] text-severity-error">
                  Confirm?
                </span>
                <button
                  onClick={() => {
                    onDelete();
                    setConfirmDelete(false);
                  }}
                  className="px-3 py-1.5 text-[0.8em] rounded bg-severity-error/15 text-severity-error hover:bg-severity-error/25 transition-colors"
                >
                  Yes
                </button>
                <button
                  onClick={() => setConfirmDelete(false)}
                  className={`px-3 py-1.5 text-[0.8em] rounded transition-colors ${c(
                    "text-text-muted hover:bg-ink-hover",
                    "text-light-text-muted hover:bg-light-hover",
                  )}`}
                >
                  No
                </button>
              </div>
            )}
          </div>
          {footer && <div className="flex items-center gap-2">{footer}</div>}
        </div>
      )}
    </ExpandableCard>
  );
}
