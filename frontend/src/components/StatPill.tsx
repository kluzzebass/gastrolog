export function StatPill({
  label,
  value,
  dark,
}: Readonly<{
  label: string;
  value: string;
  dark: boolean;
}>) {
  return (
    <div className="flex items-baseline gap-1.5">
      <span
        className={`font-mono text-[0.9em] font-medium ${dark ? "text-text-bright" : "text-light-text-bright"}`}
      >
        {value}
      </span>
      <span
        className={`text-[0.7em] uppercase tracking-wider ${dark ? "text-text-muted" : "text-light-text-muted"}`}
      >
        {label}
      </span>
    </div>
  );
}
