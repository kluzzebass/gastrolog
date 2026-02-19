import { isValidElement, useEffect, useMemo, useRef, useState } from "react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Dialog } from "./Dialog";
import { useThemeClass } from "../hooks/useThemeClass";
import { helpTopics, findTopic } from "../help/topics";
import type { HelpTopic } from "../help/topics";
import { MermaidDiagram } from "./Mermaid";
import { getHelpIcon } from "../help/icons";

interface HelpDialogProps {
  dark: boolean;
  topicId?: string;
  onClose: () => void;
  onNavigate: (topicId: string) => void;
}

/** Check if `id` is the topic or any descendant of `topic`. */
function isWithin(topic: HelpTopic, id: string): boolean {
  if (topic.id === id) return true;
  return topic.children?.some((c) => isWithin(c, id)) ?? false;
}

export function HelpDialog({ dark, topicId, onClose, onNavigate }: HelpDialogProps) {
  const c = useThemeClass(dark);
  const activeId = topicId ?? helpTopics[0]?.id ?? "";
  const [expanded, setExpanded] = useState<Set<string>>(() => {
    // Auto-expand the branch containing the initial topic
    const initial = new Set<string>();
    for (const t of helpTopics) {
      if (t.children && isWithin(t, activeId)) {
        initial.add(t.id);
      }
    }
    return initial;
  });

  // Auto-expand when navigating to a topic inside a collapsed parent
  useEffect(() => {
    setExpanded((prev) => {
      let changed = false;
      const next = new Set(prev);
      for (const t of helpTopics) {
        if (t.children && !next.has(t.id) && isWithin(t, activeId)) {
          next.add(t.id);
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [activeId]);

  const topic: HelpTopic | undefined = findTopic(activeId);
  const contentRef = useRef<HTMLDivElement>(null);

  // Reset scroll position when switching topics
  useEffect(() => {
    contentRef.current?.scrollTo(0, 0);
  }, [activeId]);

  function navigateToTopic(id: string) {
    const target = findTopic(id);
    if (target) selectTopic(target);
  }

  // Memoize so react-markdown doesn't unmount/remount custom components
  // (e.g. MermaidDiagram) on every parent re-render.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const mdComponents = useMemo(() => markdownComponents(dark, navigateToTopic), [dark, expanded]);

  function selectTopic(t: HelpTopic) {
    onNavigate(t.id);
    // Auto-expand when selecting a parent topic
    if (t.children && !expanded.has(t.id)) {
      setExpanded((prev) => new Set(prev).add(t.id));
    }
  }

  function toggleExpanded(id: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function renderTopic(t: HelpTopic, depth: number) {
    const isActive = activeId === t.id;
    const hasChildren = !!t.children;
    const isExpanded = hasChildren && expanded.has(t.id);

    return (
      <div key={t.id}>
        <button
          onClick={() => selectTopic(t)}
          className={`flex items-center w-full text-left rounded text-[0.85em] transition-colors mb-0.5 ${
            isActive
              ? "bg-copper/15 text-copper font-medium"
              : c(
                  "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                  "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                )
          }`}
          style={{ paddingLeft: `${0.5 + depth * 0.75}rem`, paddingRight: '0.5rem', paddingTop: '0.375rem', paddingBottom: '0.375rem' }}
        >
          {hasChildren && (
            <svg
              onClick={(e) => { e.stopPropagation(); toggleExpanded(t.id); }}
              className={`w-3 h-3 mr-1 shrink-0 transition-transform cursor-pointer ${isExpanded ? "rotate-90" : ""}`}
              viewBox="0 0 12 12"
              fill="currentColor"
            >
              <path d="M4.5 2l4 4-4 4" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          )}
          {t.title}
        </button>
        {isExpanded && (
          <div>
            {t.children!.map((child) => renderTopic(child, depth + 1))}
          </div>
        )}
      </div>
    );
  }

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
          {helpTopics.map((t) => renderTopic(t, 0))}
        </nav>

        {/* Content */}
        <div ref={contentRef} className="flex-1 overflow-y-auto app-scroll p-6 pt-10">
          <h2
            className={`font-display text-[1.4em] font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Help
          </h2>
          {topic ? (
            <Markdown remarkPlugins={[remarkGfm]} components={mdComponents} urlTransform={(url) => url}>
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

function markdownComponents(dark: boolean, onNavigate: (topicId: string) => void) {
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
      // Mermaid diagram
      if (className?.includes("language-mermaid")) {
        return <MermaidDiagram chart={String(children).trim()} dark={dark} />;
      }
      // Code block (inside <pre>)
      return <code className={`font-mono text-[0.85em] ${className}`}>{children}</code>;
    },
    pre: ({ children }: { children?: React.ReactNode }) => {
      // Unwrap <pre> for mermaid diagrams — the code component renders
      // them as <MermaidDiagram>, which shouldn't be wrapped in <pre>.
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
