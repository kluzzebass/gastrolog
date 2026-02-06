export function EmptyState({ dark }: { dark: boolean }) {
  return (
    <div className="flex flex-col items-center justify-center h-full py-20 animate-fade-up">
      <div
        className={`font-display text-[3em] font-light leading-none mb-3 ${dark ? "text-ink-border" : "text-light-border"}`}
      >
        &empty;
      </div>
      <p
        className={`text-[0.9em] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        Enter a query to search your logs
      </p>
      <p
        className={`text-[0.8em] mt-1 font-mono ${dark ? "text-text-ghost/60" : "text-light-text-ghost/60"}`}
      >
        press Enter to execute
      </p>
    </div>
  );
}
