import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";

interface ExpandableCardProps {
  id: string;
  typeBadge?: string;
  typeBadgeAccent?: boolean;
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
        <div className="flex items-center gap-2.5">
          <span
            className={`text-[0.7em] transition-transform ${expanded ? "rotate-90" : ""}`}
          >
            {"\u25B6"}
          </span>
          <span
            className={`${monoTitle ? "font-mono" : ""} text-[0.9em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}
          >
            {id}
          </span>
          {typeBadge && (
            <span
              className={`px-1.5 py-0.5 text-[0.8em] font-mono rounded ${
                typeBadgeAccent
                  ? "bg-copper/15 text-copper"
                  : c(
                      "bg-ink-hover text-text-muted",
                      "bg-light-hover text-light-text-muted",
                    )
              }`}
            >
              {typeBadge}
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
