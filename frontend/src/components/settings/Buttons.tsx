import { useThemeClass } from "../../hooks/useThemeClass";

interface PrimaryButtonProps {
  onClick: () => void;
  disabled?: boolean;
  children: React.ReactNode;
}

export function PrimaryButton({
  onClick,
  disabled,
  children,
}: PrimaryButtonProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
    >
      {children}
    </button>
  );
}

interface GhostButtonProps {
  onClick: () => void;
  dark: boolean;
  bordered?: boolean;
  children: React.ReactNode;
  className?: string;
}

export function GhostButton({
  onClick,
  dark,
  bordered,
  children,
  className: extra,
}: GhostButtonProps) {
  const c = useThemeClass(dark);
  return (
    <button
      onClick={onClick}
      className={`px-3 py-1.5 text-[0.8em] rounded transition-colors ${
        bordered
          ? c(
              "border border-ink-border text-text-muted hover:text-text-bright hover:bg-ink-hover",
              "border border-light-border text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
            )
          : c(
              "text-text-muted hover:text-text-bright hover:bg-ink-hover",
              "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
            )
      }${extra ? ` ${extra}` : ""}`}
    >
      {children}
    </button>
  );
}
