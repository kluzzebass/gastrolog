import { forwardRef, useDeferredValue, type ReactNode } from "react";
import { tokenize, type HighlightRole, type SyntaxSets } from "../queryTokenizer";
import { useThemeClass } from "../hooks/useThemeClass";

interface QueryInputProps {
  value: string;
  onChange: (e: React.ChangeEvent<HTMLTextAreaElement>) => void;
  onKeyDown?: (e: React.KeyboardEvent<HTMLTextAreaElement>) => void;
  onClick?: (e: React.MouseEvent<HTMLTextAreaElement>) => void;
  placeholder?: string;
  dark: boolean;
  syntax?: SyntaxSets;
  errorOffset?: number; // byte offset from backend validation (-1 = valid, -2 = use client-side)
  errorMessage?: string | null; // error message from backend validation
  children?: ReactNode;
}

function roleStyle(role: HighlightRole, dark: boolean): React.CSSProperties {
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

export const QueryInput = forwardRef<HTMLTextAreaElement, QueryInputProps>(
  (
    {
      value,
      onChange,
      onKeyDown,
      onClick,
      placeholder,
      dark,
      syntax,
      errorOffset = -2,
      errorMessage: errorMessageProp,
      children,
    },
    ref,
  ) => {
    // Tokenize a deferred copy so the textarea is never blocked by highlighting.
    const displayValue = useDeferredValue(value);
    const { spans, errorMessage: clientErrorMessage } = tokenize(displayValue, syntax, errorOffset);
    const errorMessage = errorMessageProp ?? clientErrorMessage;

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
