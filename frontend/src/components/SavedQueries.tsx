import { useEffect, useRef, useState } from "react";
import { useClickOutside } from "../hooks/useClickOutside";
import { useThemeClass } from "../hooks/useThemeClass";
import { clickableProps } from "../utils";
import type { SavedQuery } from "../api/gen/gastrolog/v1/config_pb";

export function SavedQueries({
  queries,
  dark,
  currentQuery,
  onSelect,
  onSave,
  onDelete,
  onClose,
}: {
  queries: SavedQuery[];
  dark: boolean;
  currentQuery: string;
  onSelect: (query: string) => void;
  onSave: (name: string, query: string) => void;
  onDelete: (name: string) => void;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [saveName, setSaveName] = useState("");
  const nameInputRef = useRef<HTMLInputElement>(null);
  useClickOutside(ref, onClose);

  // Auto-focus the name input.
  useEffect(() => {
    nameInputRef.current?.focus();
  }, []);

  const c = useThemeClass(dark);

  const handleSave = () => {
    const name = saveName.trim();
    if (!name) return;
    onSave(name, currentQuery);
    setSaveName("");
  };

  return (
    <div
      ref={ref}
      className={`absolute left-0 right-0 top-full mt-1 z-40 rounded border shadow-lg max-h-80 overflow-y-auto app-scroll ${c("bg-ink-surface border-ink-border", "bg-light-surface border-light-border")}`}
    >
      {/* Save current query */}
      <div
        className={`px-3 py-2 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
      >
        <div
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Save Current Query
        </div>
        <div className="flex gap-1.5">
          <input
            ref={nameInputRef}
            type="text"
            value={saveName}
            onChange={(e) => setSaveName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                handleSave();
              }
            }}
            placeholder="Query name..."
            className={`flex-1 px-2 py-1 text-[0.8em] border rounded focus:outline-none transition-colors ${c(
              "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
            )}`}
          />
          <button
            onClick={handleSave}
            disabled={!saveName.trim()}
            className="px-2.5 py-1 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-40"
          >
            Save
          </button>
        </div>
      </div>

      {/* Saved queries list */}
      {queries.length === 0 ? (
        <div
          className={`px-3 py-3 text-center text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          No saved queries yet
        </div>
      ) : (
        <>
          <div
            className={`px-3 py-1.5 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
          >
            <span
              className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              Saved Queries
            </span>
          </div>
          {queries.map((entry) => (
            <div
              key={entry.name}
              className={`group flex items-center gap-2 px-3 py-1.5 cursor-pointer transition-colors ${c("hover:bg-ink-hover", "hover:bg-light-hover")}`}
              onClick={() => onSelect(entry.query)}
              {...clickableProps(() => onSelect(entry.query))}
            >
              <div className="flex-1 min-w-0">
                <div
                  className={`text-[0.8em] font-medium truncate ${c("text-text-bright", "text-light-text-bright")}`}
                >
                  {entry.name}
                </div>
                <div
                  className={`font-mono text-[0.7em] truncate ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  {entry.query}
                </div>
              </div>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onDelete(entry.name);
                }}
                className={`opacity-0 group-hover:opacity-100 text-[0.75em] transition-opacity ${c("text-text-ghost hover:text-severity-error", "text-light-text-ghost hover:text-severity-error")}`}
                aria-label="Delete saved query"
                title="Delete saved query"
              >
                &times;
              </button>
            </div>
          ))}
        </>
      )}
    </div>
  );
}
