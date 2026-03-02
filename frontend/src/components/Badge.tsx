import { useThemeClass } from "../hooks/useThemeClass";

export type BadgeVariant =
  | "copper"
  | "debug"
  | "info"
  | "warn"
  | "error"
  | "muted"
  | "ghost";

interface BadgeProps {
  variant: BadgeVariant;
  dark: boolean;
  children: React.ReactNode;
  className?: string;
}

const variantClasses: Record<BadgeVariant, string | ((c: ReturnType<typeof useThemeClass>) => string)> = {
  copper: "bg-copper/15 text-copper",
  debug: "bg-severity-debug/15 text-severity-debug",
  info: "bg-severity-info/15 text-severity-info",
  warn: "bg-severity-warn/15 text-severity-warn",
  error: "bg-severity-error/15 text-severity-error",
  muted: (c) => c("bg-ink-hover text-text-muted", "bg-light-hover text-light-text-muted"),
  ghost: (c) => c("bg-ink-hover text-text-ghost", "bg-light-hover text-light-text-ghost"),
};

/**
 * Badge renders a small, pill-shaped label with consistent sizing and font.
 *
 * All badges use: px-1.5 py-0.5 text-[0.75em] font-mono rounded.
 * The variant controls color only.
 */
export function Badge({ variant, dark, children, className }: Readonly<BadgeProps>) {
  const c = useThemeClass(dark);
  const colorCls =
    typeof variantClasses[variant] === "function"
      ? (variantClasses[variant] as (c: ReturnType<typeof useThemeClass>) => string)(c)
      : variantClasses[variant];

  return (
    <span
      className={`px-1.5 py-0.5 text-[0.75em] font-mono rounded whitespace-nowrap ${colorCls}${className ? ` ${className}` : ""}`}
    >
      {children}
    </span>
  );
}
