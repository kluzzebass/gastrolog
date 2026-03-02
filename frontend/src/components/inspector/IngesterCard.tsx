import { useThemeClass } from "../../hooks/useThemeClass";
import { useIngesterStatus } from "../../api/hooks";
import { formatBytes } from "../../utils/units";
import { Badge } from "../Badge";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";

interface IngesterCardProps {
  ingester: { id: string; name: string; type: string; running: boolean; nodeId: string };
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  showNodeBadge?: boolean;
}

export function IngesterCard({
  ingester,
  dark,
  expanded,
  onToggle,
  showNodeBadge = true,
}: Readonly<IngesterCardProps>) {
  return (
    <ExpandableCard
      id={ingester.name || ingester.id}
      typeBadge={ingester.type}
      typeBadgeAccent
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          {showNodeBadge && <NodeBadge nodeId={ingester.nodeId} dark={dark} />}
          {ingester.running ? (
            <Badge variant="info" dark={dark}>running</Badge>
          ) : (
            <Badge variant="ghost" dark={dark}>stopped</Badge>
          )}
        </span>
      }
    >
      <IngesterDetail id={ingester.id} dark={dark} />
    </ExpandableCard>
  );
}

function IngesterDetail({ id, dark }: Readonly<{ id: string; dark: boolean }>) {
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
      hint: "No vault filter matched, or storage I/O failed",
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
