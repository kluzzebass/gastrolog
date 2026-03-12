import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { Dialog } from "../Dialog";
import {
  LockIcon,
  ServiceIcon,
  FilterIcon,
  RouteIcon,
  VaultsIcon,
  IngestersIcon,
  RetentionIcon,
  PolicyIcon,
  UsersIcon,
  LookupIcon,
  ClusterIcon,
  FilesIcon,
} from "../icons";
import { VaultsSettings } from "./VaultsSettings";
import { IngestersSettings } from "./IngestersSettings";
import { CertificatesSettings } from "./CertificatesSettings";
import { FiltersSettings } from "./FiltersSettings";
import { RoutesSettings } from "./RoutesSettings";
import { PoliciesSettings } from "./PoliciesSettings";
import { RetentionPoliciesSettings } from "./RetentionPoliciesSettings";
import { UsersSettings } from "./UsersSettings";
import { LookupsSettings } from "./LookupsSettings";
import { FilesSettings } from "./FilesSettings";
import { NodesSettings } from "./NodesSettings";
import { ServiceSettings } from "./ServiceSettings";
import { HelpButton } from "../HelpButton";

export type SettingsTab =
  | "service"
  | "nodes"
  | "certificates"
  | "files"
  | "lookups"
  | "vaults"
  | "ingesters"
  | "filters"
  | "routes"
  | "policies"
  | "retention"
  | "users";

interface SettingsDialogProps {
  dark: boolean;
  /** Tab name, optionally suffixed with `:entityName` for deep-linking (e.g. "vaults:myVault"). */
  tab: string;
  onTabChange: (tab: SettingsTab) => void;
  onClose: () => void;
  onOpenInspector?: (inspectorParam: string) => void;
  isAdmin?: boolean;
  noAuth?: boolean;
}

type TabDef = {
  id: SettingsTab;
  label: string;
  icon: (p: { className?: string }) => React.ReactNode;
  adminOnly?: boolean;
  helpTopicId?: string;
};

const allTabs: TabDef[] = [
  { id: "service", label: "Cluster", icon: ClusterIcon, helpTopicId: "service-settings" },
  { id: "nodes", label: "Nodes", icon: ServiceIcon, helpTopicId: "clustering-nodes" },
  { id: "certificates", label: "Certificates", icon: LockIcon, helpTopicId: "certificates" },
  { id: "files", label: "Files", icon: FilesIcon, helpTopicId: "managed-files" },
  { id: "lookups", label: "Lookups", icon: LookupIcon, helpTopicId: "lookups-settings" },
  { id: "users", label: "Users", icon: UsersIcon, adminOnly: true, helpTopicId: "user-management" },
  { id: "ingesters", label: "Ingesters", icon: IngestersIcon, helpTopicId: "ingesters" },
  { id: "policies", label: "Rotation Policies", icon: PolicyIcon, helpTopicId: "policy-rotation" },
  { id: "retention", label: "Retention Policies", icon: RetentionIcon, helpTopicId: "policy-retention" },
  { id: "vaults", label: "Vaults", icon: VaultsIcon, helpTopicId: "storage-engines" },
  { id: "filters", label: "Filters", icon: FilterIcon, helpTopicId: "routing" },
  { id: "routes", label: "Routes", icon: RouteIcon, helpTopicId: "routing" },
];

/** Parse tab param — may include `:entityName` for deep-linking (e.g. "vaults:myVault"). */
function parseTabParam(raw: string): { tab: SettingsTab; expandEntity: string | null } {
  const idx = raw.indexOf(":");
  if (idx >= 0) {
    const t = raw.slice(0, idx) as SettingsTab;
    return { tab: allTabs.some((d) => d.id === t) ? t : "service", expandEntity: raw.slice(idx + 1) };
  }
  const t = raw as SettingsTab;
  return { tab: allTabs.some((d) => d.id === t) ? t : "service", expandEntity: null };
}

export function SettingsDialog({
  dark,
  tab: rawTab,
  onTabChange,
  onClose,
  onOpenInspector,
  isAdmin,
  noAuth,
}: Readonly<SettingsDialogProps>) {
  const c = useThemeClass(dark);
  const tabs = allTabs.filter((t) => !t.adminOnly || isAdmin);

  // Parse optional ":entityName" suffix from the tab param.
  const { tab, expandEntity } = parseTabParam(rawTab);
  const [expandTarget, setExpandTarget] = useState<string | null>(expandEntity);

  // If the URL-provided expand entity changes (e.g. navigating from inspector), update.
  const [prevExpandEntity, setPrevExpandEntity] = useState(expandEntity);
  if (expandEntity !== prevExpandEntity) {
    setPrevExpandEntity(expandEntity);
    if (expandEntity) setExpandTarget(expandEntity);
  }

  const navigateTo = (targetTab: SettingsTab, entityName?: string) => {
    onTabChange(targetTab);
    setExpandTarget(entityName ?? null);
  };

  const clearExpandTarget = () => setExpandTarget(null);

  return (
    <Dialog onClose={onClose} ariaLabel="Settings" dark={dark}>
      <div className="flex h-full overflow-hidden">
        <nav
          className={`min-w-fit shrink-0 border-r overflow-y-auto app-scroll p-3 ${c("border-ink-border", "border-light-border")}`}
        >
          <h2
            className={`text-[0.75em] uppercase tracking-wider font-medium mb-3 px-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Settings
          </h2>
          {tabs.map(({ id, label, icon: Icon }) => (
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
          {(() => {
            const active = tabs.find((t) => t.id === tab);
            if (!active) return null;
            return (
              <div className="flex items-center gap-2 mb-5">
                <h2 className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}>
                  {active.label}
                </h2>
                {active.helpTopicId && <HelpButton topicId={active.helpTopicId} />}
              </div>
            );
          })()}
          {tab === "service" && <ServiceSettings dark={dark} noAuth={noAuth} />}
          {tab === "nodes" && <NodesSettings dark={dark} />}
          {tab === "certificates" && <CertificatesSettings dark={dark} />}
          {tab === "files" && <FilesSettings dark={dark} />}
          {tab === "lookups" && <LookupsSettings dark={dark} />}
          {tab === "users" && <UsersSettings dark={dark} noAuth={noAuth} />}
          {tab === "ingesters" && <IngestersSettings dark={dark} expandTarget={expandTarget} onExpandTargetConsumed={clearExpandTarget} onOpenInspector={onOpenInspector} />}
          {tab === "filters" && <FiltersSettings dark={dark} onNavigateTo={navigateTo} />}
          {tab === "routes" && <RoutesSettings dark={dark} onNavigateTo={navigateTo} />}
          {tab === "policies" && <PoliciesSettings dark={dark} onNavigateTo={navigateTo} />}
          {tab === "retention" && <RetentionPoliciesSettings dark={dark} onNavigateTo={navigateTo} />}
          {tab === "vaults" && <VaultsSettings dark={dark} expandTarget={expandTarget} onExpandTargetConsumed={clearExpandTarget} onOpenInspector={onOpenInspector} />}
        </div>
      </div>
    </Dialog>
  );
}
