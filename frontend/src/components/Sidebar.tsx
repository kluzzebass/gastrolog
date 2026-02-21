import { useState } from "react";
import type { FieldSummary } from "../utils";

export function SidebarSection({
  title,
  dark,
  children,
}: Readonly<{
  title: string;
  dark: boolean;
  children: React.ReactNode;
}>) {
  return (
    <section className="mb-5">
      <h3
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {title}
      </h3>
      {children}
    </section>
  );
}

export function FieldExplorer({
  fields,
  dark,
  onSelect,
  activeQuery,
}: Readonly<{
  fields: FieldSummary[];
  dark: boolean;
  onSelect: (key: string, value: string) => void;
  activeQuery: string;
}>) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  if (fields.length === 0) {
    return (
      <div
        className={`text-[0.8em] italic ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        No fields
      </div>
    );
  }

  const toggleKey = (key: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <div className="space-y-px">
      {fields.map(({ key, count, values }) => {
        const isExpanded = expanded.has(key);
        return (
          <div key={key}>
            <button
              onClick={() => toggleKey(key)}
              className={`w-full flex items-center gap-1.5 px-1.5 py-1.5 text-left text-[0.8em] rounded transition-colors ${dark ? "hover:bg-ink-hover text-text-muted hover:text-text-normal" : "hover:bg-light-hover text-light-text-muted hover:text-light-text-normal"}`}
            >
              <span
                className={`text-[0.7em] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
              >
                {isExpanded ? "\u25be" : "\u25b8"}
              </span>
              <span className="flex-1 font-mono truncate">{key}</span>
              <span
                className={`text-[0.85em] tabular-nums ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
              >
                {count}
              </span>
            </button>
            {isExpanded && (
              <div className="ml-4 space-y-px">
                {values.map(({ value, count: vCount }) => {
                  const needsQuotes = /[^a-zA-Z0-9_\-.]/.test(value);
                  const token = needsQuotes
                    ? `${key}="${value}"`
                    : `${key}=${value}`;
                  const isActive = activeQuery.includes(token);
                  return (
                    <button
                      key={value}
                      onClick={() => onSelect(key, value)}
                      className={`w-full flex items-center gap-1.5 px-1.5 py-1 text-left text-[0.75em] rounded transition-colors ${
                        isActive
                          ? dark
                            ? "bg-copper/15 text-copper"
                            : "bg-copper/10 text-copper"
                          : dark
                            ? "hover:bg-ink-hover text-text-ghost hover:text-copper-glow"
                            : "hover:bg-light-hover text-light-text-ghost hover:text-copper"
                      }`}
                    >
                      <span className="flex-1 font-mono truncate">{value}</span>
                      <span className="tabular-nums">{vCount}</span>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function StoreButton({
  label,
  count,
  active,
  onClick,
  dark,
}: Readonly<{
  label: string;
  count: string;
  active: boolean;
  onClick: () => void;
  dark: boolean;
}>) {
  return (
    <button
      onClick={onClick}
      className={`flex justify-between items-center px-2.5 py-2 text-[0.9em] rounded text-left transition-all duration-150 ${
        active
          ? dark
            ? "bg-copper/15 text-copper border border-copper/25"
            : "bg-copper/10 text-copper border border-copper/25"
          : dark
            ? "text-text-muted hover:text-text-normal hover:bg-ink-hover border border-transparent"
            : "text-light-text-muted hover:text-light-text-normal hover:bg-light-hover border border-transparent"
      }`}
    >
      <span className="font-medium">{label}</span>
      <span
        className={`font-mono text-[0.8em] ${active ? "text-copper-dim" : dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {count}
      </span>
    </button>
  );
}
