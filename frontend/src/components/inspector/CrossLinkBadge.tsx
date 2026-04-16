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
