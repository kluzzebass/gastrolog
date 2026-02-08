import type { ProtoRecord } from "../utils";
import { syntaxHighlight } from "../syntax";

export function ContextRecord({
  record,
  isAnchor,
  dark,
  onSelect,
}: {
  record: ProtoRecord;
  isAnchor: boolean;
  dark: boolean;
  onSelect?: () => void;
}) {
  const rawText = new TextDecoder().decode(record.raw);
  const parts = syntaxHighlight(rawText);
  const writeTime = record.writeTs ? record.writeTs.toDate() : new Date();

  const ts = writeTime.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
    hour12: false,
  });

  const storeId = record.ref?.storeId ?? "";

  return (
    <div
      onClick={onSelect}
      className={`grid grid-cols-[10ch_1fr] gap-x-1.5 px-2 py-0.5 text-[0.8em] leading-snug border-l-2 ${
        isAnchor
          ? dark
            ? "border-l-copper bg-copper/10 text-text-normal"
            : "border-l-copper bg-copper/8 text-light-text-normal"
          : dark
            ? "border-l-transparent text-text-ghost hover:text-text-muted"
            : "border-l-transparent text-light-text-ghost hover:text-light-text-muted"
      } ${onSelect ? "cursor-pointer" : ""}`}
    >
      <span className="font-mono text-[0.9em] tabular-nums self-center shrink-0">
        {ts}
        {storeId && (
          <span
            className={`ml-1 ${dark ? "text-text-ghost/60" : "text-light-text-ghost/60"}`}
          >
            {storeId}
          </span>
        )}
      </span>
      <span className="font-mono text-[0.9em] truncate self-center">
        {parts.map((part, i) => {
          const style = part.color ? { color: part.color } : undefined;
          return (
            <span key={i} style={style}>
              {part.text}
            </span>
          );
        })}
      </span>
    </div>
  );
}
