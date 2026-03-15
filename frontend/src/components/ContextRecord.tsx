import type { ProtoRecord } from "../utils";
import { useThemeClass } from "../hooks/useThemeClass";
import { protoToInstant, formatLocalTime } from "../utils/temporal";
import { syntaxHighlight, type HighlightMode } from "../syntax";
import { detectSeverity } from "./LogEntry";

export function ContextRecord({
  record,
  isAnchor,
  dark,
  onSelect,
  highlightMode = "full",
}: Readonly<{
  record: ProtoRecord;
  isAnchor: boolean;
  dark: boolean;
  onSelect?: () => void;
  highlightMode?: HighlightMode;
}>) {
  const c = useThemeClass(dark);
  const rawText = new TextDecoder().decode(record.raw).trimEnd();
  const parts = syntaxHighlight(rawText, highlightMode);
  const severity = detectSeverity(record.attrs);
  const writeInstant = record.writeTs ? protoToInstant(record.writeTs) : null;
  const ts = writeInstant ? formatLocalTime(writeInstant) : "--:--:--";

  const borderClass = isAnchor
    ? c("border-l-copper bg-copper/10 text-text-normal", "border-l-copper bg-copper/8 text-light-text-normal")
    : c("border-l-transparent text-text-ghost hover:text-text-muted", "border-l-transparent text-light-text-ghost hover:text-light-text-muted");

  const children = (
    <>
      <span className="font-mono text-[0.9em] tabular-nums self-center shrink-0">
        {ts}
      </span>
      <span className="self-center flex justify-center">
        {severity && (
          <span
            className={`text-[0.6em] font-semibold leading-none border rounded-sm px-0.5 py-px ${severity.cls}`}
          >
            {severity.label}
          </span>
        )}
      </span>
      <span className="font-mono text-[0.9em] truncate whitespace-pre self-center pl-1.5 text-left">
        {(() => {
          const offsets: number[] = [];
          for (let j = 0; j < parts.length; j++) {
            offsets.push(j === 0 ? 0 : offsets[j - 1]! + parts[j - 1]!.text.length);
          }
          return parts.map((part, i) => (
            <span
              key={`o${offsets[i]}`}
              style={part.color ? { color: part.color } : undefined}
            >
              {part.text}
            </span>
          ));
        })()}
      </span>
    </>
  );

  const baseClass = `grid grid-cols-[10ch_3.5ch_1fr] px-2 py-0.5 text-[0.8em] leading-snug border-l-2 ${borderClass}`;

  if (onSelect) {
    return (
      <button type="button" onClick={onSelect} className={`${baseClass} cursor-pointer w-full text-left`}>
        {children}
      </button>
    );
  }

  return <div className={baseClass}>{children}</div>;
}
