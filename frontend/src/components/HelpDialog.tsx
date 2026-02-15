import { useState } from "react";
import Markdown from "react-markdown";
import { Dialog } from "./Dialog";
import { useThemeClass } from "../hooks/useThemeClass";
import { helpTopics, findTopic } from "../help/topics";
import type { HelpTopic } from "../help/topics";

interface HelpDialogProps {
  dark: boolean;
  topicId?: string;
  onClose: () => void;
}

export function HelpDialog({ dark, topicId, onClose }: HelpDialogProps) {
  const c = useThemeClass(dark);
  const [activeId, setActiveId] = useState(
    () => topicId ?? helpTopics[0]?.id ?? "",
  );

  const topic: HelpTopic | undefined = findTopic(activeId);

  return (
    <Dialog onClose={onClose} ariaLabel="Help" dark={dark} size="xl">
      <div className="flex h-full overflow-hidden">
        {/* Sidebar */}
        <nav
          className={`w-48 shrink-0 border-r overflow-y-auto app-scroll p-3 ${c("border-ink-border", "border-light-border")}`}
        >
          <h2
            className={`text-[0.75em] uppercase tracking-wider font-medium mb-3 px-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Topics
          </h2>
          {helpTopics.map((t) => (
            <button
              key={t.id}
              onClick={() => setActiveId(t.id)}
              className={`block w-full text-left px-2 py-1.5 rounded text-[0.85em] transition-colors mb-0.5 ${
                activeId === t.id
                  ? "bg-copper/15 text-copper font-medium"
                  : c(
                      "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                      "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                    )
              }`}
            >
              {t.title}
            </button>
          ))}
        </nav>

        {/* Content */}
        <div className="flex-1 overflow-y-auto app-scroll p-6">
          {topic ? (
            <Markdown components={markdownComponents(c)}>
              {topic.content}
            </Markdown>
          ) : (
            <p
              className={`text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              Select a topic from the sidebar.
            </p>
          )}
        </div>

        {/* Close button */}
        <button
          onClick={onClose}
          aria-label="Close"
          className={`absolute top-3 right-3 w-7 h-7 flex items-center justify-center rounded text-lg leading-none transition-colors ${c("text-text-muted hover:text-text-bright", "text-light-text-muted hover:text-light-text-bright")}`}
        >
          &times;
        </button>
      </div>
    </Dialog>
  );
}

type C = ReturnType<typeof useThemeClass>;

function markdownComponents(c: C) {
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
      // Inline code (no language class)
      if (!className) {
        return (
          <code
            className={`font-mono text-[0.9em] px-1.5 py-0.5 rounded ${c("bg-ink-surface text-text-normal", "bg-light-hover text-light-text-normal")}`}
          >
            {children}
          </code>
        );
      }
      // Code block (inside <pre>)
      return <code className={`font-mono text-[0.85em] ${className}`}>{children}</code>;
    },
    pre: ({ children }: { children?: React.ReactNode }) => (
      <pre
        className={`mb-3 p-3 rounded overflow-x-auto app-scroll text-[0.9em] ${c("bg-ink-surface", "bg-light-hover")}`}
      >
        {children}
      </pre>
    ),
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
    }) => (
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="text-copper hover:underline"
      >
        {children}
      </a>
    ),
    table: ({ children }: { children?: React.ReactNode }) => (
      <table
        className={`mb-3 w-full text-[0.9em] border-collapse ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {children}
      </table>
    ),
    th: ({ children }: { children?: React.ReactNode }) => (
      <th
        className={`text-left py-1.5 px-2 border-b font-medium text-[0.85em] uppercase tracking-wider ${c("border-ink-border text-text-ghost", "border-light-border text-light-text-ghost")}`}
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
  };
}
