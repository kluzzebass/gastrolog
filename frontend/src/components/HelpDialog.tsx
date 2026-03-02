import { isValidElement, lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import { Dialog } from "./Dialog";
import { useThemeClass } from "../hooks/useThemeClass";
import { helpTopics, findTopic, resolveTopicId, allTopics } from "../help/topics";
import type { HelpTopic } from "../help/topics";
import { getHelpIcon } from "../help/icons";

const Markdown = lazy(() => import("react-markdown"));
const MermaidDiagram = lazy(() => import("./Mermaid").then((m) => ({ default: m.MermaidDiagram })));

let remarkGfmPlugin: any[] = [];
import("remark-gfm").then((m) => { remarkGfmPlugin = [m.default]; });

/** Stable identity transform — hoisted to module level to avoid re-renders. */
const identityUrlTransform = (url: string) => url;

interface HelpDialogProps {
  dark: boolean;
  topicId?: string;
  onClose: () => void;
  onNavigate: (topicId: string) => void;
  onOpenSettings?: (tab: string) => void;
}

/** Check if `id` is the topic or any descendant of `topic`. */
function isWithin(topic: HelpTopic, id: string): boolean {
  if (topic.id === id) return true;
  return topic.children?.some((c) => isWithin(c, id)) ?? false;
}

/** Strip markdown syntax to plain text for search indexing. */
function stripMarkdown(md: string): string {
  return md
    .replace(/```[\s\S]*?```/g, " ")       // fenced code blocks
    .replace(/`[^`]+`/g, " ")              // inline code
    // eslint-disable-next-line sonarjs/slow-regex -- bounded character classes, no backtracking risk
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, "$1") // links/images → text
    .replace(/#{1,6}\s+/g, " ")            // headings
    .replace(/[*_~|>-]+/g, " ")            // emphasis, tables, blockquotes
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

/** Pre-built search index entry. */
interface SearchEntry {
  topic: HelpTopic;
  titleLower: string;
  plainText: string;
}

export function HelpDialog({ dark, topicId, onClose, onNavigate, onOpenSettings }: Readonly<HelpDialogProps>) {
  const c = useThemeClass(dark);
  const activeId = resolveTopicId(topicId ?? helpTopics[0]?.id ?? "");
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

  const [search, setSearch] = useState("");
  const searchRef = useRef<HTMLInputElement>(null);

  // Lazily build search index — only loads all content when user starts searching
  const [searchIndex, setSearchIndex] = useState<SearchEntry[] | null>(null);
  const searchIndexLoading = useRef(false);
  useEffect(() => {
    if (!search.trim() || searchIndex || searchIndexLoading.current) return;
    searchIndexLoading.current = true;
    const topics = allTopics();
    Promise.all(topics.map((t) => t.load().then((content) => ({
      topic: t,
      titleLower: t.title.toLowerCase(),
      plainText: stripMarkdown(content),
    })))).then(setSearchIndex);
  }, [search, searchIndex]);

  const searchResults = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q || !searchIndex) return null;
    const terms = q.split(/\s+/);
    return searchIndex.filter((entry) =>
      terms.every((term) => entry.titleLower.includes(term) || entry.plainText.includes(term)),
    );
  }, [search, searchIndex]);

  // Auto-expand and reset content when navigating to a new topic.
  const [prevActiveId, setPrevActiveId] = useState(activeId);
  const topic: HelpTopic | undefined = findTopic(activeId);
  const contentRef = useRef<HTMLDivElement>(null);
  const [topicContent, setTopicContent] = useState<string | null>(null);
  const [loadingContent, setLoadingContent] = useState(true);

  if (activeId !== prevActiveId) {
    setPrevActiveId(activeId);
    setLoadingContent(true);
    setTopicContent(null);
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
  }

  // Load topic content and reset scroll when switching topics.
  useEffect(() => {
    contentRef.current?.scrollTo(0, 0);
    if (!topic) return;
    let cancelled = false;
    topic.load().then((content) => {
      if (!cancelled) {
        setTopicContent(content);
        setLoadingContent(false);
      }
    });
    return () => { cancelled = true; };
  }, [activeId, topic]);

  const navigate = (id: string) => {
    const target = findTopic(id);
    if (target) {
      onNavigate(target.id);
      if (target.children) {
        setExpanded((prev) => new Set(prev).add(target.id));
      }
    }
  };

  function selectTopic(t: HelpTopic) {
    onNavigate(t.id);
    setSearch("");
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

  function topicButtonCls(isActive: boolean, isTopLevel: boolean): string {
    if (isActive) return "bg-copper/15 text-copper";
    if (isTopLevel) return c("text-text-bright hover:bg-ink-hover", "text-light-text-bright hover:bg-light-hover");
    return c(
      "text-text-muted hover:text-text-bright hover:bg-ink-hover",
      "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
    );
  }

  function renderTopic(t: HelpTopic, depth: number, index: number) {
    const isActive = activeId === t.id;
    const hasChildren = !!t.children;
    const isExpanded = hasChildren && expanded.has(t.id);
    const isTopLevel = depth === 0;

    return (
      <div key={t.id} className={isTopLevel && index > 0 ? "mt-1.5" : undefined}>
        <button
          onClick={() => selectTopic(t)}
          className={`flex items-center w-full text-left rounded transition-colors mb-0.5 ${
            isTopLevel ? "text-[0.85em] font-medium" : "text-[0.8em]"
          } ${
            topicButtonCls(isActive, isTopLevel)
          }`}
          style={{ paddingLeft: `${0.5 + depth * 0.75}rem`, paddingRight: '0.5rem', paddingTop: '0.375rem', paddingBottom: '0.375rem' }}
        >
          <span className="w-3 h-3 mr-1 shrink-0 flex items-center justify-center">
            {hasChildren && (
              <svg
                onClick={(e) => { e.stopPropagation(); toggleExpanded(t.id); }}
                onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); e.stopPropagation(); toggleExpanded(t.id); } }}
                role="button"
                tabIndex={0}
                aria-label={isExpanded ? "Collapse section" : "Expand section"}
                className={`w-3 h-3 transition-transform cursor-pointer ${isExpanded ? "rotate-90" : ""}`}
                viewBox="0 0 12 12"
                fill="currentColor"
              >
                <path d="M4.5 2l4 4-4 4" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            )}
          </span>
          {t.title}
        </button>
        {isExpanded && (
          <div>
            {t.children!.map((child, i) => renderTopic(child, depth + 1, i))}
          </div>
        )}
      </div>
    );
  }

  /** Extract a short snippet around the first match in the content. */
  function getSnippet(entry: SearchEntry): string | null {
    const q = search.trim().toLowerCase();
    if (!q || entry.titleLower.includes(q)) return null;
    const terms = q.split(/\s+/);
    let idx = -1;
    for (const term of terms) {
      const i = entry.plainText.indexOf(term);
      if (i !== -1 && (idx < 0 || i < idx)) idx = i;
    }
    if (idx < 0) return null;
    const start = Math.max(0, idx - 30);
    const end = Math.min(entry.plainText.length, idx + 60);
    return (start > 0 ? "..." : "") + entry.plainText.slice(start, end) + (end < entry.plainText.length ? "..." : "");
  }


  function renderContent() {
    if (topic && topicContent && !loadingContent) {
      return (
        <MarkdownContent
          dark={dark}
          content={topicContent}
          onNavigate={navigate}
          onOpenSettings={onOpenSettings}
        />
      );
    }
    if (topic) {
      return (
        <p className={`text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
          Loading...
        </p>
      );
    }
    return (
      <p className={`text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
        Select a topic from the sidebar.
      </p>
    );
  }

  function renderSidebarContent() {
    if (!searchResults) {
      return (
        <>
          <h2
            className={`text-[0.75em] uppercase tracking-wider font-medium mb-2 px-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Topics
          </h2>
          {helpTopics.map((t, i) => renderTopic(t, 0, i))}
        </>
      );
    }
    if (searchResults.length === 0) {
      return (
        <p className={`text-[0.8em] px-2 py-1 ${c("text-text-ghost", "text-light-text-ghost")}`}>
          No results
        </p>
      );
    }
    return (
      <>
        {searchResults.map((entry) => {
          const snippet = getSnippet(entry);
          return (
            <button
              key={entry.topic.id}
              onClick={() => selectTopic(entry.topic)}
              className={`flex flex-col w-full text-left rounded text-[0.85em] transition-colors mb-0.5 px-2 py-1.5 ${
                activeId === entry.topic.id
                  ? "bg-copper/15 text-copper font-medium"
                  : c(
                      "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                      "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                    )
              }`}
            >
              <span>{entry.topic.title}</span>
              {snippet && (
                <span className={`text-[0.8em] truncate ${c("text-text-ghost", "text-light-text-ghost")}`}>
                  {snippet}
                </span>
              )}
            </button>
          );
        })}
      </>
    );
  }
  return (
    <Dialog onClose={onClose} ariaLabel="Help" dark={dark} size="xl">
      <div className="flex h-full overflow-hidden">
        {/* Sidebar */}
        <nav
          className={`w-48 shrink-0 border-r overflow-y-auto app-scroll flex flex-col ${c("border-ink-border", "border-light-border")}`}
        >
          {/* Search */}
          <div className="p-3 pb-0">
            <div className="relative mb-3">
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className={`absolute left-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 pointer-events-none ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                <circle cx="11" cy="11" r="8" />
                <line x1="21" y1="21" x2="16.65" y2="16.65" />
              </svg>
              <input
                ref={searchRef}
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search help..."
                className={`w-full pl-7 pr-2 py-1.5 text-[0.8em] rounded border focus:outline-none focus:border-copper ${c(
                  "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost",
                  "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost",
                )}`}
              />
              {search && (
                <button
                  onClick={() => { setSearch(""); searchRef.current?.focus(); }}
                  className={`absolute right-1.5 top-1/2 -translate-y-1/2 w-4 h-4 flex items-center justify-center rounded-sm text-[0.7em] leading-none ${c("text-text-ghost hover:text-text-muted", "text-light-text-ghost hover:text-light-text-muted")}`}
                  aria-label="Clear search"
                >
                  &times;
                </button>
              )}
            </div>
          </div>

          {/* Topic tree or search results */}
          <div className="flex-1 overflow-y-auto app-scroll px-3 pb-3">
            {renderSidebarContent()}
          </div>
        </nav>

        {/* Content */}
        <div ref={contentRef} className="flex-1 overflow-y-auto app-scroll p-6">
          {renderContent()}
        </div>

      </div>
    </Dialog>
  );
}

/**
 * Isolated markdown renderer. Callbacks are stored in refs so the components
 * object (and thus react-markdown's DOM) only changes when `dark` flips.
 * Parent re-renders (e.g. polling in SearchView) no longer cause remounts
 * that reset scroll positions inside <pre> blocks.
 */
function MarkdownContent({ dark, content, onNavigate, onOpenSettings }: Readonly<{
  dark: boolean;
  content: string;
  onNavigate: (topicId: string) => void;
  onOpenSettings?: (tab: string) => void;
}>) {
  // React Compiler handles memoization — components object only rebuilds
  // when dark/onNavigate/onOpenSettings actually change.
  const components = buildMarkdownComponents(dark, onNavigate, onOpenSettings);

  return (
    <Suspense fallback={null}>
      <Markdown remarkPlugins={remarkGfmPlugin} components={components} urlTransform={identityUrlTransform}>
        {content}
      </Markdown>
    </Suspense>
  );
}

/** Coerce React children to a plain string for code/mermaid blocks. */
function childrenToText(children: React.ReactNode): string {
  if (typeof children === "string") return children;
  if (children == null) return "";
  return `${children as string | number}`;
}

function buildMarkdownComponents(
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
        // Fenced block without language — contains newlines.
        // Don't apply inline padding (px/py) here; <pre> handles the box.
        // The inline <code> padding causes a visual leading-space on the
        // first line because padding only applies to the start of the
        // inline element, not to each wrapped line.
        if (text.includes("\n")) {
          return (
            <code className={`font-mono text-[0.85em] ${c("text-text-normal", "text-light-text-normal")}`}>
              {text.replace(/\n$/, "")}
            </code>
          );
        }
        // Inline code
        return (
          <code
            className={`font-mono text-[0.9em] px-1.5 py-0.5 rounded ${c("bg-ink-surface text-text-normal", "bg-light-hover text-light-text-normal")}`}
          >
            {children}
          </code>
        );
      }
      // Mermaid diagram
      if (className.includes("language-mermaid")) {
        const chart = childrenToText(children);
        return <Suspense fallback={<div className="py-4 text-center text-text-ghost text-[0.85em]">Loading diagram...</div>}><MermaidDiagram chart={chart.trim()} dark={dark} /></Suspense>;
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
