import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";
import { Badge } from "../Badge";
import { HelpButton } from "../HelpButton";

/**
 * A header badge: bare text, or text with the meaning on hover and a help
 * reference to open — a badge that carries a state the operator must
 * understand is never a bare word.
 */
export type BadgeSpec = string | { text: string; title?: string; helpRef?: string };

function badgeText(b: BadgeSpec): string {
  return typeof b === "string" ? b : b.text;
}

function badgeTitle(b: BadgeSpec): string | undefined {
  return typeof b === "string" ? undefined : b.title;
}

function badgeHelpRef(b: BadgeSpec): string | undefined {
  return typeof b === "string" ? undefined : b.helpRef;
}

interface ExpandableCardProps {
  id: string;
  typeBadge?: BadgeSpec;
  typeBadgeAccent?: boolean;
  secondaryBadge?: BadgeSpec;
  /** Help reference opened from a button in the header, beside the title. */
  helpRef?: string;
  dark: boolean;
  expanded?: boolean;
  onToggle?: () => void;
  children: React.ReactNode;
  status?: React.ReactNode;
  headerRight?: React.ReactNode;
  monoTitle?: boolean;
}

export function ExpandableCard({
  id,
  typeBadge,
  typeBadgeAccent,
  secondaryBadge,
  helpRef,
  dark,
  expanded,
  onToggle,
  children,
  status,
  headerRight,
  monoTitle = true,
}: Readonly<ExpandableCardProps>) {
  const c = useThemeClass(dark);

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
        <div className="flex items-center gap-2.5 min-w-0">
          <span
            className={`text-[0.7em] transition-transform ${expanded ? "rotate-90" : ""} ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {"\u25B6"}
          </span>
          <span
            className={`${monoTitle ? "font-mono" : ""} text-[0.9em] font-medium truncate ${c("text-text-bright", "text-light-text-bright")}`}
            title={id}
          >
            {id}
          </span>
          {typeBadge && (
            <Badge
              variant={typeBadgeAccent ? "copper" : "muted"}
              dark={dark}
              title={badgeTitle(typeBadge)}
              helpRef={badgeHelpRef(typeBadge)}
            >
              {badgeText(typeBadge)}
            </Badge>
          )}
          {secondaryBadge && (
            <Badge
              variant="muted"
              dark={dark}
              title={badgeTitle(secondaryBadge)}
              helpRef={badgeHelpRef(secondaryBadge)}
            >
              {badgeText(secondaryBadge)}
            </Badge>
          )}
          {helpRef && (
            <span onClick={(e) => e.stopPropagation()} onKeyDown={(e) => e.stopPropagation()} role="presentation">
              <HelpButton helpRef={helpRef} />
            </span>
          )}
          {status}
        </div>
        {headerRight}
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
