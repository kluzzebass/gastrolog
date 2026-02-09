import { useRef } from "react";
import { useClickOutside } from "../hooks/useClickOutside";
import { useThemeClass } from "../hooks/useThemeClass";
import type { HistoryEntry } from "../hooks/useQueryHistory";

export function QueryHistory({
  entries,
  dark,
  onSelect,
  onRemove,
  onClear,
  onClose,
}: {
  entries: HistoryEntry[];
  dark: boolean;
  onSelect: (query: string) => void;
  onRemove: (query: string) => void;
  onClear: () => void;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useClickOutside(ref, onClose);

  const c = useThemeClass(dark);

  if (entries.length === 0) return null;

  return (
    <div
      ref={ref}
      className={`absolute left-0 right-0 top-full mt-1 z-40 rounded border shadow-lg max-h-64 overflow-y-auto app-scroll ${c("bg-ink-surface border-ink-border", "bg-light-surface border-light-border")}`}
    >
      <div
        className={`flex items-center justify-between px-3 py-1.5 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
      >
        <span
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Recent queries
        </span>
        <button
          onClick={onClear}
          className={`text-[0.7em] transition-colors ${c("text-text-ghost hover:text-severity-error", "text-light-text-ghost hover:text-severity-error")}`}
        >
          Clear
        </button>
      </div>
      {entries.map((entry) => (
        <div
          key={entry.query}
          className={`group flex items-center gap-2 px-3 py-1.5 cursor-pointer transition-colors ${c("hover:bg-ink-hover", "hover:bg-light-hover")}`}
          onClick={() => onSelect(entry.query)}
        >
          <span
            className={`flex-1 font-mono text-[0.8em] truncate ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {entry.query}
          </span>
          <button
            onClick={(e) => {
              e.stopPropagation();
              onRemove(entry.query);
            }}
            className={`opacity-0 group-hover:opacity-100 text-[0.75em] transition-opacity ${c("text-text-ghost hover:text-severity-error", "text-light-text-ghost hover:text-severity-error")}`}
            title="Remove from history"
          >
            &times;
          </button>
        </div>
      ))}
    </div>
  );
}
