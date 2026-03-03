import { useThemeClass } from "../hooks/useThemeClass";

export function LoadingPlaceholder({
  dark,
  className,
}: Readonly<{ dark: boolean; className?: string }>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")} ${className ?? ""}`}
    >
      Loading...
    </div>
  );
}
