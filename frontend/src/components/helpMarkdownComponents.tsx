import { isValidElement, lazy, Suspense } from "react";
import { getHelpIcon } from "../help/icons";

const MermaidDiagram = lazy(() => import("./Mermaid").then((m) => ({ default: m.MermaidDiagram })));

/** Coerce React children to a plain string for code/mermaid blocks. */
function childrenToText(children: React.ReactNode): string {
  if (typeof children === "string") return children;
  if (children == null) return "";
  return `${children as string | number}`;
}

export function buildMarkdownComponents(
  dark: boolean,
  onNavigate: (topicId: string) => void,
  onOpenSettings?: (tab: string) => void,
) {
  const c: (d: string, l: string) => string = dark
    ? (d) => d
    : (_, l) => l;
  return {
    h1: ({ children }: { children?: React.ReactNode }) => (
      <h1
        className={`font-display text-[1.4em] font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}
      >
        {children}
      </h1>
    ),
    h2: ({ children }: { children?: React.ReactNode }) => (
      <h2
        className={`font-display text-[1.1em] font-semibold mt-6 mb-2 ${c("text-copper", "text-copper")}`}
      >
        {children}
      </h2>
    ),
    h3: ({ children }: { children?: React.ReactNode }) => (
      <h3
        className={`font-display text-[0.95em] font-semibold mt-4 mb-2 ${c("text-text-bright", "text-light-text-bright")}`}
      >
        {children}
      </h3>
    ),
    p: ({ children }: { children?: React.ReactNode }) => (
      <p
        className={`text-[0.9em] mb-3 leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {children}
      </p>
    ),
    ul: ({ children }: { children?: React.ReactNode }) => (
      <ul className="mb-3 ml-4 list-disc space-y-1">{children}</ul>
    ),
    ol: ({ children }: { children?: React.ReactNode }) => (
      <ol className="mb-3 ml-4 list-decimal space-y-1">{children}</ol>
    ),
    li: ({ children }: { children?: React.ReactNode }) => (
      <li
        className={`text-[0.9em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {children}
      </li>
    ),
    code: ({
      children,
      className,
    }: {
      children?: React.ReactNode;
      className?: string;
    }) => {
      if (!className) {
        const text = childrenToText(children);
        if (text.includes("\n")) {
          return (
            <code className={`font-mono text-[0.85em] ${c("text-text-normal", "text-light-text-normal")}`}>
              {text.replace(/\n$/, "")}
            </code>
          );
        }
        return (
          <code
            className={`font-mono text-[0.9em] px-1.5 py-0.5 rounded ${c("bg-ink-surface text-text-normal", "bg-light-hover text-light-text-normal")}`}
          >
            {children}
          </code>
        );
      }
      if (className.includes("language-mermaid")) {
        const chart = childrenToText(children);
        return <Suspense fallback={<div className="py-4 text-center text-text-muted text-[0.85em]">Loading diagram...</div>}><MermaidDiagram chart={chart.trim()} dark={dark} /></Suspense>;
      }
      return <code className={`font-mono text-[0.85em] ${c("text-text-normal", "text-light-text-normal")} ${className}`}>{children}</code>;
    },
    pre: ({ children }: { children?: React.ReactNode }) => {
      if (
        isValidElement<{ className?: string }>(children) &&
        children.props.className?.includes("language-mermaid")
      ) {
        return <>{children}</>;
      }
      return (
        <pre
          className={`mb-3 p-3 rounded overflow-x-auto app-scroll text-[0.9em] ${c("bg-ink-surface", "bg-light-hover")}`}
        >
          {children}
        </pre>
      );
    },
    strong: ({ children }: { children?: React.ReactNode }) => (
      <strong
        className={`font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
      >
        {children}
      </strong>
    ),
    em: ({ children }: { children?: React.ReactNode }) => (
      <em className="italic">{children}</em>
    ),
    a: ({
      href,
      children,
    }: {
      href?: string;
      children?: React.ReactNode;
    }) => {
      if (href?.startsWith("help:")) {
        const topicId = href.slice(5);
        return (
          <button
            onClick={() => onNavigate(topicId)}
            className="text-copper hover:underline cursor-pointer"
          >
            {children}
          </button>
        );
      }
      if (href?.startsWith("settings:")) {
        const tab = href.slice(9);
        return (
          <button
            onClick={() => onOpenSettings?.(tab)}
            className="text-copper hover:underline cursor-pointer"
          >
            {children}
          </button>
        );
      }
      return (
        <a
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          className="text-copper hover:underline"
        >
          {children}
        </a>
      );
    },
    table: ({ children }: { children?: React.ReactNode }) => (
      <table
        className={`mb-3 w-full text-[0.9em] border-collapse ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {children}
      </table>
    ),
    th: ({ children }: { children?: React.ReactNode }) => (
      <th
        className={`text-left py-1.5 px-2 border-b font-medium text-[0.85em] uppercase tracking-wider ${c("border-ink-border text-text-muted", "border-light-border text-light-text-muted")}`}
      >
        {children}
      </th>
    ),
    td: ({ children }: { children?: React.ReactNode }) => (
      <td
        className={`py-1.5 px-2 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
      >
        {children}
      </td>
    ),
    hr: () => (
      <hr
        className={`my-4 border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
      />
    ),
    img: ({ alt }: { alt?: string }) => {
      if (alt?.startsWith("icon:")) {
        const icon = getHelpIcon(alt.slice(5));
        if (icon) {
          return (
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className={`inline-block w-4 h-4 align-text-bottom ${c("text-copper", "text-copper")}`}
            >
              {icon}
            </svg>
          );
        }
      }
      return null;
    },
  };
}
