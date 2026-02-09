import { useEffect } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
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
  const c = useThemeClass(dark);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    };
    window.addEventListener("keydown", handler, true);
    return () => window.removeEventListener("keydown", handler, true);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      onClick={onClose}
    >
      <div className="absolute inset-0 bg-black/40" />
      <div
        className={`relative w-[90vw] max-w-5xl h-[85vh] flex flex-col rounded-lg shadow-2xl overflow-hidden ${c("bg-ink-raised border border-ink-border-subtle", "bg-light-raised border border-light-border-subtle")}`}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          className={`flex items-center gap-4 px-5 py-3 border-b shrink-0 ${c("border-ink-border", "border-light-border")}`}
        >
          <h2
            className={`font-display text-[1.2em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Inspector
          </h2>

          {/* Tabs */}
          <div className="flex gap-1 ml-4">
            {allTabs.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                onClick={() => onTabChange(id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-[0.8em] transition-colors ${
                  tab === id
                    ? "bg-copper/15 text-copper font-medium"
                    : c(
                        "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                        "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                      )
                }`}
              >
                <Icon className="w-3.5 h-3.5" />
                {label}
              </button>
            ))}
          </div>

          <div className="flex-1" />

          <button
            onClick={onClose}
            className={`w-7 h-7 flex items-center justify-center rounded text-lg leading-none transition-colors ${c("text-text-muted hover:text-text-bright", "text-light-text-muted hover:text-light-text-bright")}`}
          >
            &times;
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-5">
          {tab === "stores" && <StoresPanel dark={dark} />}
          {tab === "ingesters" && <IngestersPanel dark={dark} />}
          {tab !== "stores" && tab !== "ingesters" && (
            <Placeholder tab={tab} dark={dark} />
          )}
        </div>
      </div>
    </div>
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

// --- Tab Icons ---

function StoresIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
    </svg>
  );
}

function IngestersIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <path d="M12 3v12" />
      <path d="M8 11l4 4 4-4" />
      <path d="M3 17h18" />
      <path d="M3 21h18" />
    </svg>
  );
}

function MetricsIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <path d="M3 3v18h18" />
      <path d="M7 17l4-8 4 4 5-9" />
    </svg>
  );
}
