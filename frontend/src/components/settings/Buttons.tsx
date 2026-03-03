import { useThemeClass } from "../../hooks/useThemeClass";

type ButtonVariant = "primary" | "ghost" | "danger";

interface ButtonProps {
  onClick: () => void;
  dark?: boolean;
  variant?: ButtonVariant;
  bordered?: boolean;
  disabled?: boolean;
  children: React.ReactNode;
  className?: string;
}

const baseClass = "px-3 py-1.5 text-[0.8em] rounded transition-colors disabled:opacity-50";

export function Button({
  onClick,
  dark = true,
  variant = "primary",
  bordered,
  disabled,
  children,
  className: extra,
}: Readonly<ButtonProps>) {
  const c = useThemeClass(dark);

  let variantClass: string;
  switch (variant) {
    case "primary":
      variantClass = "bg-copper text-white hover:bg-copper-glow";
      break;
    case "danger":
      variantClass = "bg-red-700 text-white hover:bg-red-600";
      break;
    case "ghost":
      variantClass = bordered
        ? c(
            "border border-ink-border text-text-muted hover:text-text-bright hover:bg-ink-hover",
            "border border-light-border text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
          )
        : c(
            "text-text-muted hover:text-text-bright hover:bg-ink-hover",
            "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
          );
      break;
  }

  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`${baseClass} ${variantClass}${extra ? " " + extra : ""}`}
    >
      {children}
    </button>
  );
}

