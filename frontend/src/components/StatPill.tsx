import { LiveValue } from "./LiveValue";

export function StatPill({
  label,
  value,
  reserve,
  dark,
}: Readonly<{
  label: string;
  value: string;
  /** Character cells to reserve for the value: its formatter's `*_MAX_CHARS` contract. */
  reserve?: number;
  dark: boolean;
}>) {
  return (
    <div className="flex items-baseline gap-1.5">
      <LiveValue dark={dark} reserve={reserve} className="text-[0.9em] font-medium">
        {value}
      </LiveValue>
      <span
        className={`text-[0.7em] uppercase tracking-wider ${dark ? "text-text-muted" : "text-light-text-muted"}`}
      >
        {label}
      </span>
    </div>
  );
}
