import { forwardRef, useDeferredValue, type ReactNode } from "react";
import { useThemeClass } from "../hooks/useThemeClass";

interface QueryInputProps {
  value: string;
  onChange: (e: React.ChangeEvent<HTMLTextAreaElement>) => void;
  onKeyDown?: (e: React.KeyboardEvent<HTMLTextAreaElement>) => void;
  onClick?: (e: React.MouseEvent<HTMLTextAreaElement>) => void;
  placeholder?: string;
  dark: boolean;
  highlightSpans?: Array<{ text: string; role: string }>;
  highlightExpression?: string; // which expression spans are for
  errorMessage?: string | null;
  children?: ReactNode;
}

function roleStyle(role: string, dark: boolean): React.CSSProperties {
  switch (role) {
    case "operator":
      return { color: "var(--color-severity-warn)", fontWeight: 700 };
    case "directive-key":
      return { color: "var(--color-copper)", fontStyle: "italic" };
    case "key":
      return { color: "var(--color-copper)" };
    case "eq":
    case "compare-op":
      return { color: "var(--color-text-ghost)" };
    case "value":
    case "token":
      return {
        color: dark
          ? "var(--color-text-bright)"
          : "var(--color-light-text-bright)",
      };
    case "quoted":
      return { color: "var(--color-severity-info)" };
    case "glob":
      return { color: "var(--color-severity-debug)", fontStyle: "italic" };
    case "regex":
      return { color: "var(--color-severity-debug)", fontStyle: "italic" };
    case "paren":
      return { color: "var(--color-text-ghost)" };
    case "star":
      return { color: "var(--color-severity-debug)" };
    case "pipe":
      return { color: "var(--color-severity-warn)", fontWeight: 700 };
    case "pipe-keyword":
      return { color: "var(--color-severity-warn)", fontWeight: 700 };
    case "function":
      return { color: "var(--color-copper)" };
    case "comma":
      return { color: "var(--color-text-ghost)" };
    case "comment":
      return { color: "var(--color-text-ghost)", fontStyle: "italic" };
    case "error":
      return {
        color: "var(--color-severity-error)",
        textDecoration: "underline wavy",
      };
    case "whitespace":
      return {};
    default:
      return {};
  }
}

/** Number of leading characters shared by both strings. */
function commonPrefixLen(a: string, b: string): number {
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    if (a[i] !== b[i]) return i;
  }
  return len;
}

/** Number of trailing characters shared by both strings, not overlapping `prefixLen`. */
function commonSuffixLen(a: string, b: string, prefixLen: number): number {
  let i = 0;
  const maxLen = Math.min(a.length, b.length) - prefixLen;
  while (i < maxLen && a[a.length - 1 - i] === b[b.length - 1 - i]) {
    i++;
  }
  return i;
}

type Span = { text: string; role: string };

/** Trim spans to keep only the first `len` characters. */
function takeSpans(spans: Span[], len: number): Span[] {
  const result: Span[] = [];
  let remaining = len;
  for (const span of spans) {
    if (remaining <= 0) break;
    if (span.text.length <= remaining) {
      result.push(span);
      remaining -= span.text.length;
    } else {
      result.push({ text: span.text.slice(0, remaining), role: span.role });
      remaining = 0;
    }
  }
  return result;
}

/** Drop the first `len` characters from spans and return the remainder. */
function dropSpans(spans: Span[], len: number): Span[] {
  const result: Span[] = [];
  let skip = len;
  for (const span of spans) {
    if (skip >= span.text.length) {
      skip -= span.text.length;
      continue;
    }
    if (skip > 0) {
      result.push({ text: span.text.slice(skip), role: span.role });
      skip = 0;
    } else {
      result.push(span);
    }
  }
  return result;
}

/**
 * Build the overlay spans for the current displayValue.
 *
 * - Fresh match (backend spans match displayValue exactly) → use them.
 * - Stale → find common prefix and suffix between the old expression and
 *   the new text, keep colored spans for both ends, fill the changed middle
 *   with neutral "token" color.
 * - No spans at all → entire text in neutral.
 */
function resolveSpans(
  displayValue: string,
  highlightSpans: Span[] | undefined,
  highlightExpression: string | undefined,
): Span[] {
  const hasSpans = highlightSpans && highlightSpans.length > 0;

  // Fresh match — backend spans cover this exact text.
  if (hasSpans && highlightExpression === displayValue) {
    return highlightSpans;
  }

  // Stale spans — reuse prefix + suffix that still match.
  if (hasSpans && highlightExpression) {
    const pfx = commonPrefixLen(highlightExpression, displayValue);
    const sfx = commonSuffixLen(highlightExpression, displayValue, pfx);

    const result: Span[] = [];

    if (pfx > 0) {
      result.push(...takeSpans(highlightSpans, pfx));
    }

    // Middle section: characters in displayValue between prefix and suffix.
    const middleLen = displayValue.length - pfx - sfx;
    if (middleLen > 0) {
      result.push({ text: displayValue.slice(pfx, pfx + middleLen), role: "token" });
    }

    if (sfx > 0) {
      // Suffix spans: drop the first (staleLen - sfx) chars from the stale spans.
      result.push(...dropSpans(highlightSpans, highlightExpression.length - sfx));
    }

    if (result.length > 0) return result;
  }

  // Nothing usable — show entire text unstyled.
  if (displayValue) return [{ text: displayValue, role: "token" }];
  return [];
}

export const QueryInput = forwardRef<HTMLTextAreaElement, QueryInputProps>(
  (
    {
      value,
      onChange,
      onKeyDown,
      onClick,
      placeholder,
      dark,
      highlightSpans,
      highlightExpression,
      errorMessage,
      children,
    },
    ref,
  ) => {
    // Use a deferred copy so the textarea is never blocked by highlighting.
    const displayValue = useDeferredValue(value);
    const spans = resolveSpans(displayValue, highlightSpans, highlightExpression);

    const c = useThemeClass(dark);

    return (
      <>
        {/* Textarea — identical to original except transparent text + caret */}
        <textarea
          ref={ref}
          value={value}
          onChange={onChange}
          onKeyDown={onKeyDown}
          onClick={onClick}
          spellCheck={false}
          rows={1}
          placeholder={placeholder}
          title={errorMessage ?? undefined}
          style={
            {
              fieldSizing: "content",
              color: "transparent",
              caretColor: dark
                ? "var(--color-text-bright)"
                : "var(--color-light-text-bright)",
            } as React.CSSProperties
          }
          className={`query-input w-full pl-3 pr-14 py-[8.5px] text-[0.9em] leading-normal font-mono border rounded resize-none overflow-hidden focus:outline-none ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost",
          )} selection:bg-copper/30`}
        />

        {/* Highlight overlay — sits on top of textarea, passes clicks through */}
        <div
          aria-hidden
          className="absolute inset-0 pl-3 pr-14 py-[8.5px] text-[0.9em] leading-normal font-mono whitespace-pre-wrap overflow-hidden pointer-events-none"
          style={{ borderWidth: 1, borderColor: "transparent" }}
        >
          {spans.map((span, i) => (
            <span key={`${i}-${span.role}`} style={roleStyle(span.role, dark)}>
              {span.text}
            </span>
          ))}
        </div>

        {/* Overlay buttons (history, help) */}
        {children && (
          <div className="absolute right-2 top-2.5 flex items-center gap-1">
            {children}
          </div>
        )}
      </>
    );
  },
);

QueryInput.displayName = "QueryInput";
