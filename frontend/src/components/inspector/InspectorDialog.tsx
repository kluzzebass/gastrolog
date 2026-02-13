import { useThemeClass } from "../../hooks/useThemeClass";
import { Dialog, DialogTabHeader } from "../Dialog";
import { StoresIcon, IngestersIcon, MetricsIcon } from "../icons";
import { StoresPanel } from "./StoresPanel";
import { IngestersPanel } from "./IngestersPanel";

export type InspectorTab = "stores" | "ingesters" | "metrics";

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
  { id: "metrics", label: "Metrics", icon: MetricsIcon },
];

export function InspectorDialog({
  dark,
  tab,
  onTabChange,
  onClose,
}: InspectorDialogProps) {
  return (
    <Dialog onClose={onClose} ariaLabel="Inspector" dark={dark}>
      <DialogTabHeader
        title="Inspector"
        tabs={allTabs}
        activeTab={tab}
        onTabChange={(t) => onTabChange(t as InspectorTab)}
        onClose={onClose}
        dark={dark}
      />

      <div className="flex-1 overflow-y-auto p-5">
        {tab === "stores" && <StoresPanel dark={dark} />}
        {tab === "ingesters" && <IngestersPanel dark={dark} />}
        {tab !== "stores" && tab !== "ingesters" && (
          <Placeholder tab={tab} dark={dark} />
        )}
      </div>
    </Dialog>
  );
}

function Placeholder({ tab, dark }: { tab: InspectorTab; dark: boolean }) {
  const c = useThemeClass(dark);
  const labels: Record<InspectorTab, string> = {
    stores: "Store health indicators will appear here.",
    ingesters: "Ingester metrics will appear here.",
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

