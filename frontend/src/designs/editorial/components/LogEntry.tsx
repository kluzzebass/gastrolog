import type { ProtoRecord } from "../utils";
import { highlightMatches } from "../utils";

export function LogEntry({
  record,
  tokens,
  isSelected,
  onSelect,
  dark,
}: {
  record: ProtoRecord;
  tokens: string[];
  isSelected: boolean;
  onSelect: () => void;
  dark: boolean;
}) {
  const rawText = new TextDecoder().decode(record.raw);
  const parts = highlightMatches(rawText, tokens);
  const ingestTime = record.ingestTs ? record.ingestTs.toDate() : new Date();

  const ts = ingestTime.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
    hour12: false,
  });

  return (
    <article
      onClick={onSelect}
      className={`grid grid-cols-[13ch_1fr] gap-2.5 px-5 py-[5px] border-b cursor-pointer transition-colors duration-100 ${
        isSelected
          ? dark
            ? "bg-ink-hover"
            : "bg-light-hover"
          : dark
            ? "hover:bg-ink-surface border-b-ink-border-subtle"
            : "hover:bg-light-hover border-b-light-border-subtle"
      }`}
    >
      <span
        className={`font-mono text-[0.8em] tabular-nums self-center ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {ts}
      </span>
      <div
        className={`font-mono text-[0.85em] leading-relaxed truncate self-center ${dark ? "text-text-normal" : "text-light-text-normal"}`}
      >
        {parts.map((part, i) => (
          <span
            key={i}
            className={
              part.highlighted
                ? dark
                  ? "bg-highlight-bg border border-highlight-border text-highlight-text px-0.5 rounded-sm"
                  : "bg-light-highlight-bg border border-light-highlight-border text-light-highlight-text px-0.5 rounded-sm"
                : ""
            }
          >
            {part.text}
          </span>
        ))}
      </div>
    </article>
  );
}
