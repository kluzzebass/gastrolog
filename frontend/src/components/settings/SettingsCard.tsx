import { useState } from "react";
import { useReadOnly } from "../../hooks/useReadOnly";
import { useThemeClass } from "../../hooks/useThemeClass";
import { ExpandableCard, type BadgeSpec } from "./ExpandableCard";

interface SettingsCardProps {
  id: string;
  typeBadge?: BadgeSpec;
  secondaryBadge?: BadgeSpec;
  helpRef?: string;
  dark: boolean;
  expanded?: boolean;
  onToggle?: () => void;
  onDelete?: () => void;
  deleteLabel?: string;
  /** One line stating why Delete is refused; the button is disabled while set. */
  deleteDisabledReason?: string;
  /** What a Yes will do, one line each, listed before the confirm row. */
  deleteConsequences?: string[];
  deleteConfirmExtra?: React.ReactNode;
  children: React.ReactNode;
  status?: React.ReactNode;
  headerRight?: React.ReactNode;
  footer?: React.ReactNode;
}

export function SettingsCard({
  id,
  typeBadge,
  secondaryBadge,
  helpRef,
  dark,
  expanded,
  onToggle,
  onDelete,
  deleteLabel,
  deleteDisabledReason,
  deleteConsequences,
  deleteConfirmExtra,
  children,
  status,
  headerRight,
  footer,
}: Readonly<SettingsCardProps>) {
  const c = useThemeClass(dark);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const readOnly = useReadOnly();

  // The whole strip exists to modify this entity: the footer holds Save and
  // Discard, the left side holds Delete. A caller who cannot write is shown
  // the card's contents and none of that.
  const showActions = !readOnly && (onDelete || footer);

  return (
    <ExpandableCard
      id={id}
      typeBadge={typeBadge}
      secondaryBadge={secondaryBadge}
      helpRef={helpRef}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      status={status}
      headerRight={headerRight}
    >
      {children}
      {showActions && (
        <div className="flex items-center justify-between pt-3 mt-3">
          <div>
            {onDelete && !confirmDelete && (
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setConfirmDelete(true)}
                  disabled={!!deleteDisabledReason}
                  className={`px-3 py-1.5 text-[0.8em] rounded transition-colors disabled:opacity-50 ${c(
                    "text-text-muted hover:text-severity-error hover:bg-ink-hover",
                    "text-light-text-muted hover:text-severity-error hover:bg-light-hover",
                  )}`}
                >
                  {deleteLabel || "Delete"}
                </button>
                {deleteDisabledReason && (
                  <span className={`text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}>
                    {deleteDisabledReason}
                  </span>
                )}
              </div>
            )}
            {onDelete && confirmDelete && (
              <div className="flex flex-col gap-1.5">
                {deleteConsequences && deleteConsequences.length > 0 && (
                  <ul className={`text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}>
                    {deleteConsequences.map((line) => (
                      <li key={line}>{line}</li>
                    ))}
                  </ul>
                )}
                {deleteConfirmExtra}
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
              </div>
            )}
          </div>
          {footer && <div className="flex items-center gap-2">{footer}</div>}
        </div>
      )}
    </ExpandableCard>
  );
}
