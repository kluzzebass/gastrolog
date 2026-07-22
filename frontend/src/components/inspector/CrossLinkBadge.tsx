import { useThemeClass } from "../../hooks/useThemeClass";

interface CrossLinkBadgeProps {
  dark: boolean;
  title: string;
  onClick: () => void;
  children: React.ReactNode;
}

export function CrossLinkBadge({ dark, title, onClick, children }: Readonly<CrossLinkBadgeProps>) {
  const c = useThemeClass(dark);
  return (
    <button
      type="button"
      title={title}
      onClick={(e) => {
        e.stopPropagation();
        onClick();
      }}
      className={`inline-flex items-center justify-center w-5 h-5 rounded transition-colors ${c(
        "text-text-muted hover:text-copper hover:bg-ink-hover",
        "text-light-text-muted hover:text-copper hover:bg-light-hover",
      )}`}
    >
      {children}
    </button>
  );
}

/**
 * CrossLinkChip is the labeled counterpart to CrossLinkBadge: a Badge-shaped
 * pill that navigates instead of a plain icon button. Used where a card
 * links to another entity's card by name (e.g. a storage's placed vaults)
 * rather than opening Settings.
 */
export function CrossLinkChip({ dark, title, onClick, children }: Readonly<CrossLinkBadgeProps>) {
  const c = useThemeClass(dark);
  return (
    <button
      type="button"
      title={title}
      onClick={(e) => {
        e.stopPropagation();
        onClick();
      }}
      className={`px-1.5 py-0.5 text-[0.75em] font-mono rounded whitespace-nowrap transition-colors ${c(
        "bg-ink-hover text-text-muted hover:text-copper",
        "bg-light-hover text-light-text-muted hover:text-copper",
      )}`}
    >
      {children}
    </button>
  );
}
