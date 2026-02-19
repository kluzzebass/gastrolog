import { useThemeClass } from "../../hooks/useThemeClass";
import { Dialog } from "../Dialog";
import { StoresIcon, IngestersIcon, JobsIcon, MetricsIcon } from "../icons";
import { StoresPanel } from "./StoresPanel";
import { IngestersPanel } from "./IngestersPanel";
import { JobsPanel } from "./JobsPanel";

export type InspectorTab = "stores" | "ingesters" | "jobs" | "metrics";

interface InspectorDialogProps {
  dark: boolean;
  tab: InspectorTab;
  onTabChange: (tab: InspectorTab) => void;
  onClose: () => void;
}

type TabDef = {
  id: InspectorTab;
  label: string;
  icon: (p: { className?: string }) => React.ReactNode;
};

const allTabs: TabDef[] = [
  { id: "stores", label: "Stores", icon: StoresIcon },
  { id: "ingesters", label: "Ingesters", icon: IngestersIcon },
  { id: "jobs", label: "Jobs", icon: JobsIcon },
  { id: "metrics", label: "Metrics", icon: MetricsIcon },
];

export function InspectorDialog({
  dark,
  tab,
  onTabChange,
  onClose,
}: Readonly<InspectorDialogProps>) {
  const c = useThemeClass(dark);

  return (
    <Dialog onClose={onClose} ariaLabel="Inspector" dark={dark}>
      <div className="flex h-full overflow-hidden">
        <nav
          className={`min-w-fit shrink-0 border-r overflow-y-auto app-scroll p-3 ${c("border-ink-border", "border-light-border")}`}
        >
          <h2
            className={`text-[0.75em] uppercase tracking-wider font-medium mb-3 px-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Inspector
          </h2>
          {allTabs.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => onTabChange(id)}
              className={`flex items-center gap-2 w-full text-left px-2 py-1.5 mb-0.5 rounded text-[0.85em] transition-colors ${
                tab === id
                  ? "bg-copper/15 text-copper"
                  : c(
                      "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                      "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                    )
              }`}
            >
              <Icon className="w-3.5 h-3.5 shrink-0" />
              <span className="whitespace-nowrap">{label}</span>
            </button>
          ))}
        </nav>

        <div className="flex-1 overflow-y-auto app-scroll p-5 pt-10">
          {tab === "stores" && <StoresPanel dark={dark} />}
          {tab === "ingesters" && <IngestersPanel dark={dark} />}
          {tab === "jobs" && <JobsPanel dark={dark} />}
          {tab !== "stores" && tab !== "ingesters" && tab !== "jobs" && (
            <Placeholder tab={tab} dark={dark} />
          )}
        </div>

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

function Placeholder({ tab, dark }: Readonly<{ tab: InspectorTab; dark: boolean }>) {
  const c = useThemeClass(dark);
  const labels: Record<InspectorTab, string> = {
    stores: "Store health indicators will appear here.",
    ingesters: "Ingester metrics will appear here.",
    jobs: "Job status will appear here.",
    metrics: "Dashboard metrics will appear here.",
  };

  return (
    <div
      className={`flex items-center justify-center h-full text-[0.9em] ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      {labels[tab]}
    </div>
  );
}

