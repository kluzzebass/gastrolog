import { useThemeClass } from "../hooks/useThemeClass";

interface EnvironmentBannerProps {
  label: string;
  color: string;
  dark: boolean;
}

/**
 * Displayed in the header so operators can tell at a glance which deployment
 * they're connected to (gastrolog-4vr0l). The label and color come from the
 * api node's `--environment-label` / `--environment-color` flags, surfaced on
 * `GetSystemResponse`. Empty label collapses to nothing — callers must guard
 * the conditional render themselves; this component assumes label is set.
 *
 * Invalid CSS color strings are silently ignored by the browser, falling back
 * to the palette's text-bright color (no crash, no validation needed).
 */
export function EnvironmentBanner({
  label,
  color,
  dark,
}: Readonly<EnvironmentBannerProps>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`font-display text-[1.6em] font-bold tracking-tight leading-none uppercase ${c(
        "text-text-bright",
        "text-light-text-bright",
      )}`}
      style={color ? { color } : undefined}
      title={`Environment: ${label}`}
    >
      {label}
    </div>
  );
}
