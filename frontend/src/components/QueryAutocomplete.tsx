import { useEffect, useRef } from "react";
import { useClickOutside } from "../hooks/useClickOutside";
import { useThemeClass } from "../hooks/useThemeClass";

export function QueryAutocomplete({
  suggestions,
  selectedIndex,
  dark,
  onSelect,
  onClose,
}: {
  suggestions: string[];
  selectedIndex: number;
  dark: boolean;
  onSelect: (index: number) => void;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const selectedRef = useRef<HTMLDivElement>(null);
  useClickOutside(ref, onClose);

  // Scroll selected item into view.
  useEffect(() => {
    selectedRef.current?.scrollIntoView({ block: "nearest" });
  }, [selectedIndex]);

  const c = useThemeClass(dark);

  if (suggestions.length === 0) return null;

  return (
    <div
      ref={ref}
      className={`absolute left-0 right-0 top-full mt-1 z-40 rounded border shadow-lg max-h-48 overflow-y-auto app-scroll ${c("bg-ink-surface border-ink-border", "bg-light-surface border-light-border")}`}
    >
      {suggestions.map((suggestion, i) => (
        <div
          key={suggestion}
          ref={i === selectedIndex ? selectedRef : undefined}
          className={`px-3 py-1.5 font-mono text-[0.8em] cursor-pointer transition-colors ${
            i === selectedIndex
              ? c(
                  "bg-ink-hover text-text-bright",
                  "bg-light-hover text-light-text-bright",
                )
              : c(
                  "text-text-muted hover:bg-ink-hover",
                  "text-light-text-muted hover:bg-light-hover",
                )
          }`}
          onMouseDown={(e) => {
            e.preventDefault(); // prevent textarea blur
            onSelect(i);
          }}
        >
          {suggestion}
        </div>
      ))}
    </div>
  );
}
