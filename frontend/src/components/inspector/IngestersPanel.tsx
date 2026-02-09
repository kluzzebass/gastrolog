import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { clickableProps } from "../../utils";
import { useIngesters, useIngesterStatus } from "../../api/hooks";

export function IngestersPanel({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: ingesters, isLoading } = useIngesters();
  const [expanded, setExpanded] = useState<string | null>(null);

  if (isLoading) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading...
      </div>
    );
  }

  if (!ingesters || ingesters.length === 0) {
    return (
      <div
        className={`flex items-center justify-center h-full text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        No ingesters configured.
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {ingesters.map((ing) => (
        <IngesterCard
          key={ing.id}
          id={ing.id}
          type={ing.type}
          running={ing.running}
          dark={dark}
          expanded={expanded === ing.id}
          onToggle={() => setExpanded(expanded === ing.id ? null : ing.id)}
        />
      ))}
    </div>
  );
}

function IngesterCard({
  id,
  type,
  running,
  dark,
  expanded,
  onToggle,
}: {
  id: string;
  type: string;
  running: boolean;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
}) {
  const c = useThemeClass(dark);

  return (
    <div
      className={`border rounded-lg overflow-hidden transition-colors ${c(
        "border-ink-border-subtle bg-ink-surface",
        "border-light-border-subtle bg-light-surface",
      )}`}
    >
      {/* Header */}
      <div
        className={`flex items-center justify-between px-4 py-3 cursor-pointer select-none transition-colors ${c(
          "hover:bg-ink-hover",
          "hover:bg-light-hover",
        )}`}
        onClick={onToggle}
        {...clickableProps(onToggle)}
        aria-expanded={expanded}
      >
        <div className="flex items-center gap-2.5">
          <span
            className={`text-[0.7em] transition-transform ${expanded ? "rotate-90" : ""}`}
          >
            {"\u25B6"}
          </span>
          <span
            className={`font-mono text-[0.9em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}
          >
            {id}
          </span>
          {type && (
            <span className="px-1.5 py-0.5 text-[0.75em] rounded bg-copper/15 text-copper">
              {type}
            </span>
          )}
          <span>
            {running ? (
              <span className="px-1.5 py-0.5 text-[0.75em] rounded bg-severity-info/15 text-severity-info">
                running
              </span>
            ) : (
              <span
                className={`px-1.5 py-0.5 text-[0.75em] rounded ${c("bg-ink-hover text-text-ghost", "bg-light-hover text-light-text-ghost")}`}
              >
                stopped
              </span>
            )}
          </span>
        </div>
      </div>

      {/* Detail */}
      {expanded && (
        <div
          className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
        >
          <IngesterDetail id={id} dark={dark} />
        </div>
      )}
    </div>
  );
}

function IngesterDetail({ id, dark }: { id: string; dark: boolean }) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useIngesterStatus(id);

  if (isLoading) {
    return (
      <div
        className={`px-4 py-3 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading...
      </div>
    );
  }

  if (!data) {
    return (
      <div
        className={`px-4 py-3 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        No status available.
      </div>
    );
  }

  const stats = [
    {
      label: "Messages ingested",
      value: Number(data.messagesIngested).toLocaleString(),
    },
    { label: "Bytes ingested", value: formatBytes(Number(data.bytesIngested)) },
    {
      label: "Errors",
      value: Number(data.errors).toLocaleString(),
      isError: Number(data.errors) > 0,
    },
  ];

  return (
    <div className={`px-4 py-3 ${c("bg-ink-raised", "bg-light-bg")}`}>
      <div
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Metrics
      </div>
      <div className="flex flex-col gap-1.5">
        {stats.map((stat) => (
          <div
            key={stat.label}
            className="flex items-center gap-3 text-[0.85em]"
          >
            <span
              className={`w-36 ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {stat.label}
            </span>
            <span
              className={`font-mono ${
                stat.isError
                  ? "text-severity-error"
                  : c("text-text-bright", "text-light-text-bright")
              }`}
            >
              {stat.value}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024)
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}
