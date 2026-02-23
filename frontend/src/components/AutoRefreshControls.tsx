import { useThemeClass } from "../hooks/useThemeClass";

export const POLL_OPTIONS: { label: string; ms: number | null }[] = [
  { label: "Off", ms: null },
  { label: "5s", ms: 5_000 },
  { label: "10s", ms: 10_000 },
  { label: "30s", ms: 30_000 },
  { label: "1m", ms: 60_000 },
];

export function AutoRefreshControls({
  pollInterval,
  onPollIntervalChange,
  dark,
}: {
  pollInterval: number | null;
  onPollIntervalChange: (ms: number | null) => void;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`flex items-center rounded overflow-hidden border ${c(
        "border-ink-border-subtle",
        "border-light-border-subtle",
      )}`}
    >
      {POLL_OPTIONS.map((opt) => (
        <button
          key={opt.label}
          onClick={() => onPollIntervalChange(opt.ms)}
          className={`px-2 py-1 text-[0.75em] font-mono transition-colors ${
            pollInterval === opt.ms
              ? `${c("bg-copper/20 text-copper", "bg-copper/20 text-copper")}`
              : `${c(
                  "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                  "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                )}`
          }`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
