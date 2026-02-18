import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useIngesters, useIngesterStatus } from "../../api/hooks";
import { formatBytes } from "../../utils/units";
import { ExpandableCard } from "../settings/ExpandableCard";
import { HelpButton } from "../HelpButton";

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
      <div className="flex items-center gap-2 mb-2">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Ingesters
        </h2>
        <HelpButton topicId="inspector-ingesters" />
      </div>
      {ingesters.map((ing) => (
        <ExpandableCard
          key={ing.id}
          id={ing.name || ing.id}
          typeBadge={ing.type}
          typeBadgeAccent
          dark={dark}
          expanded={expanded === ing.id}
          onToggle={() => setExpanded(expanded === ing.id ? null : ing.id)}
          status={
            ing.running ? (
              <span className="px-1.5 py-0.5 text-[0.75em] rounded bg-severity-info/15 text-severity-info">
                running
              </span>
            ) : (
              <span
                className={`px-1.5 py-0.5 text-[0.75em] rounded ${c("bg-ink-hover text-text-ghost", "bg-light-hover text-light-text-ghost")}`}
              >
                stopped
              </span>
            )
          }
        >
          <IngesterDetail id={ing.id} dark={dark} />
        </ExpandableCard>
      ))}
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
      label: "Dropped",
      hint: "No store filter matched, or storage I/O failed",
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
            className="flex items-start gap-3 text-[0.85em]"
          >
            <div className="w-36">
              <span
                className={c("text-text-muted", "text-light-text-muted")}
              >
                {stat.label}
              </span>
              {stat.hint && (
                <div className={`text-[0.8em] leading-tight mt-0.5 ${c("text-text-ghost", "text-light-text-ghost")}`}>
                  {stat.hint}
                </div>
              )}
            </div>
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

