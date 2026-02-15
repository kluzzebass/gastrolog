import { useEffect } from "react";
import FocusTrap from "focus-trap-react";
import { useThemeClass } from "../hooks/useThemeClass";

export function QueryHelp({
  dark,
  onClose,
  onExample,
}: {
  dark: boolean;
  onClose: () => void;
  onExample: (ex: string) => void;
}) {
  const c = useThemeClass(dark);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    };
    window.addEventListener("keydown", handler, true);
    return () => window.removeEventListener("keydown", handler, true);
  }, [onClose]);

  const section = (title: string, children: React.ReactNode) => (
    <div className="mb-5 last:mb-0">
      <h3
        className={`text-[0.8em] uppercase tracking-wider font-medium mb-2 ${c("text-copper", "text-copper")}`}
      >
        {title}
      </h3>
      {children}
    </div>
  );

  const row = (filter: string, desc: string) => (
    <div className="flex gap-3 py-0.5 text-[0.85em]">
      <code
        className={`shrink-0 w-48 font-mono ${c("text-text-normal", "text-light-text-normal")}`}
      >
        {filter}
      </code>
      <span className={c("text-text-muted", "text-light-text-muted")}>
        {desc}
      </span>
    </div>
  );

  const example = (ex: string, desc: string) => (
    <div className="flex gap-3 py-0.5 text-[0.85em]">
      <button
        onClick={() => onExample(ex)}
        className={`shrink-0 font-mono text-left transition-colors ${c(
          "text-text-normal hover:text-copper",
          "text-light-text-normal hover:text-copper",
        )}`}
      >
        {ex}
      </button>
      <span className={c("text-text-ghost", "text-light-text-ghost")}>
        {desc}
      </span>
    </div>
  );

  return (
    <FocusTrap focusTrapOptions={{ escapeDeactivates: false, allowOutsideClick: true }}>
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      onClick={onClose}
    >
      <div className={`absolute inset-0 ${c("bg-black/60", "bg-black/40")}`} />
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Query Language"
        className={`relative max-w-160 w-full mx-4 max-h-[80vh] overflow-y-auto app-scroll rounded-lg border p-6 ${c(
          "bg-ink-surface border-ink-border-subtle",
          "bg-light-surface border-light-border-subtle",
        )}`}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-5">
          <h2
            className={`text-[1.1em] font-medium ${c("text-text-normal", "text-light-text-normal")}`}
          >
            Query Language
          </h2>
          <button
            onClick={onClose}
            aria-label="Close"
            className={`text-[1.2em] leading-none px-1 transition-colors ${c(
              "text-text-ghost hover:text-text-muted",
              "text-light-text-ghost hover:text-light-text-muted",
            )}`}
          >
            &times;
          </button>
        </div>

        {section(
          "Token search",
          <>
            <p
              className={`text-[0.85em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Bare words filter by token. Multiple tokens use AND semantics.
            </p>
            {row("error", 'Records containing "error"')}
            {row(
              "error timeout",
              'Records containing both "error" and "timeout"',
            )}
          </>,
        )}

        {section(
          "Boolean operators",
          <>
            <p
              className={`text-[0.85em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Combine filters with boolean logic. AND binds tighter than OR.
            </p>
            {row("error AND warn", "Explicit AND (same as implicit)")}
            {row("error OR warn", "Either token matches")}
            {row("NOT debug", 'Exclude records with "debug"')}
            {row("(error OR warn) AND NOT debug", "Parentheses for grouping")}
          </>,
        )}

        {section(
          "Key=Value filters",
          <>
            <p
              className={`text-[0.85em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Filter by key=value in record attributes or message body.
            </p>
            {row("level=error", "Exact key=value match")}
            {row(
              'key="value with spaces"',
              "Quoted values for special characters",
            )}
            {row("host=*", "Key exists with any value")}
            {row("*=error", "Value exists under any key")}
          </>,
        )}

        {section(
          "Time bounds",
          <>
            <p
              className={`text-[0.85em] mb-2 ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Filter by timestamp. Accepts RFC3339 or Unix timestamps.
            </p>
            {row("start=TIME", "Inclusive lower bound on WriteTS")}
            {row("end=TIME", "Exclusive upper bound on WriteTS")}
            {row(
              "source_start=TIME",
              "Lower bound on SourceTS (log origin time)",
            )}
            {row("source_end=TIME", "Upper bound on SourceTS")}
            {row(
              "ingest_start=TIME",
              "Lower bound on IngestTS (receiver time)",
            )}
            {row("ingest_end=TIME", "Upper bound on IngestTS")}
          </>,
        )}

        {section(
          "Result control",
          <>
            {row("limit=N", "Maximum number of results")}
            {row("reverse=true", "Return results newest-first")}
          </>,
        )}

        {section(
          "Scoping",
          <>
            {row("store=NAME", "Search only the named store")}
            {row("chunk=ID", "Search only the named chunk")}
            {row("pos=N", "Exact record position within a chunk")}
          </>,
        )}

        {section(
          "Examples",
          <>
            {example("error timeout", "Token search")}
            {example("level=error host=*", "KV filter with wildcard")}
            {example("(error OR warn) AND NOT debug", "Boolean expression")}
            {example("store=prod level=error", "Scoped search")}
            {example("reverse=true limit=50 level=error", "Latest 50 errors")}
          </>,
        )}
      </div>
    </div>
    </FocusTrap>
  );
}
