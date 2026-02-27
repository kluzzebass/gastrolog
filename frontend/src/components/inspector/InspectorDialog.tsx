import { useThemeClass } from "../../hooks/useThemeClass";
import { Dialog } from "../Dialog";
import { VaultsIcon, IngestersIcon, JobsIcon, MetricsIcon, ClusterIcon } from "../icons";
import { VaultsPanel } from "./VaultsPanel";
import { IngestersPanel } from "./IngestersPanel";
import { JobsPanel } from "./JobsPanel";
import { MetricsPanel } from "./MetricsPanel";
import { ClusterPanel } from "./ClusterPanel";

export type InspectorTab = "vaults" | "ingesters" | "jobs" | "metrics" | "cluster";

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
  { id: "vaults", label: "Vaults", icon: VaultsIcon },
  { id: "ingesters", label: "Ingesters", icon: IngestersIcon },
  { id: "jobs", label: "Jobs", icon: JobsIcon },
  { id: "metrics", label: "Metrics", icon: MetricsIcon },
  { id: "cluster", label: "Cluster", icon: ClusterIcon },
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

        <div className="flex-1 overflow-y-auto app-scroll p-5">
          {tab === "vaults" && <VaultsPanel dark={dark} />}
          {tab === "ingesters" && <IngestersPanel dark={dark} />}
          {tab === "jobs" && <JobsPanel dark={dark} />}
          {tab === "metrics" && <MetricsPanel dark={dark} />}
          {tab === "cluster" && <ClusterPanel dark={dark} />}
        </div>
      </div>
    </Dialog>
  );
}


