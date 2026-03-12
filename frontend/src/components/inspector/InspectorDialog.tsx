import { useThemeClass } from "../../hooks/useThemeClass";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useConfig } from "../../api/hooks/useConfig";
import { useSettings } from "../../api/hooks/useSettings";
import { useVaults, useIngesters } from "../../api/hooks";
import { useWatchJobs } from "../../api/hooks";
import { toastError } from "../Toast";
import { ClusterNodeRole, ClusterNodeSuffrage } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { Dialog } from "../Dialog";
import { VaultsIcon, IngestersIcon, JobsIcon, MetricsIcon, ClusterIcon, RouteIcon } from "../icons";
import { Badge } from "../Badge";
import { ModeToggle } from "./ModeToggle";
import type { InspectorMode } from "./ModeToggle";
import { NodeDetailPane } from "./NodeDetailPane";
import { EntityListPane } from "./EntityListPane";

export type EntityType = "vaults" | "ingesters" | "routes" | "jobs" | "system";

interface InspectorDialogProps {
  dark: boolean;
  inspectorParam: string;
  onNavigate: (param: string) => void;
  onClose: () => void;
  onOpenSettings?: (tab: string, entityName?: string) => void;
}

// ---- URL state parsing ----

type ParsedState =
  | { mode: "nodes"; nodeId: string }
  | { mode: "entities"; entityType: EntityType; expandTarget: string | null };

const entityTypes = new Set<EntityType>(["vaults", "ingesters", "routes", "jobs", "system"]);

function parseParam(param: string): ParsedState {
  if (param.startsWith("nodes:")) {
    return { mode: "nodes", nodeId: param.slice(6) };
  }
  if (param.startsWith("entities:")) {
    const rest = param.slice(9);
    // Format: "entities:<type>" or "entities:<type>:<entityName>"
    const colonIdx = rest.indexOf(":");
    const etRaw = colonIdx >= 0 ? rest.slice(0, colonIdx) : rest;
    const entityName = colonIdx >= 0 ? rest.slice(colonIdx + 1) : null;
    const et = etRaw as EntityType;
    if (entityTypes.has(et)) {
      return { mode: "entities", entityType: et, expandTarget: entityName };
    }
    return { mode: "entities", entityType: "vaults", expandTarget: null };
  }
  // Legacy tab names.
  switch (param) {
    case "cluster":
      return { mode: "nodes", nodeId: "" };
    case "metrics":
      return { mode: "entities", entityType: "system", expandTarget: null };
    case "vaults":
    case "ingesters":
    case "jobs":
      return { mode: "entities", entityType: param, expandTarget: null };
    default:
      return { mode: "entities", entityType: "vaults", expandTarget: null };
  }
}

function encodeParam(state: { mode: "nodes"; nodeId: string } | { mode: "entities"; entityType: EntityType }): string {
  if (state.mode === "nodes") {
    return `nodes:${state.nodeId}`;
  }
  return `entities:${state.entityType}`;
}

// ---- Entity type nav definitions ----

type EntityNavItem = {
  id: EntityType;
  label: string;
  icon: (p: { className?: string }) => React.ReactNode;
};

const entityNavItems: EntityNavItem[] = [
  { id: "vaults", label: "Vaults", icon: VaultsIcon },
  { id: "ingesters", label: "Ingesters", icon: IngestersIcon },
  { id: "routes", label: "Routes", icon: RouteIcon },
  { id: "jobs", label: "Jobs", icon: JobsIcon },
  { id: "system", label: "System", icon: MetricsIcon },
];

// ---- Dialog ----

export function InspectorDialog({
  dark,
  inspectorParam,
  onNavigate,
  onClose,
  onOpenSettings,
}: Readonly<InspectorDialogProps>) {
  const c = useThemeClass(dark);

  // Cluster/node context.
  const { data: cluster } = useClusterStatus();
  const { data: settingsData } = useSettings();
  const { data: config } = useConfig();
  const localNodeId = settingsData?.nodeId ?? "";
  const clusterEnabled = cluster?.clusterEnabled ?? false;
  const multiNode = clusterEnabled || (config?.nodeConfigs && config.nodeConfigs.length > 1);

  // Entity counts for nav badges.
  const { data: vaults } = useVaults();
  const { data: ingesters } = useIngesters();
  const { jobs } = useWatchJobs({ onError: toastError });

  const routeCount = config?.routes.length ?? 0;
  const entityCounts: Record<EntityType, number> = {
    vaults: vaults?.length ?? 0,
    ingesters: ingesters?.length ?? 0,
    routes: routeCount,
    jobs: jobs.length,
    system: cluster?.nodes.length ?? 1,
  };

  // Parse URL state, forcing entities mode in single-node.
  let parsed = parseParam(inspectorParam);
  if (!multiNode && parsed.mode === "nodes") {
    parsed = { mode: "entities", entityType: "vaults", expandTarget: null };
  }

  const mode: InspectorMode = parsed.mode;

  // Resolve selected node ID: default to local node if empty.
  const selectedNodeId =
    parsed.mode === "nodes" ? (parsed.nodeId || localNodeId) : "";

  // Node list for node names.
  const nodes = cluster?.nodes
    ? [...cluster.nodes].sort((a, b) => {
        if (a.id === localNodeId) return -1;
        if (b.id === localNodeId) return 1;
        return (a.name || "").localeCompare(b.name || "");
      })
    : [];

  return (
    <Dialog onClose={onClose} ariaLabel="Inspector" dark={dark}>
      <div className="flex h-full overflow-hidden">
        {/* ---- Left nav pane ---- */}
        <nav
          className={`min-w-fit shrink-0 border-r overflow-y-auto app-scroll p-3 flex flex-col gap-1 ${c("border-ink-border", "border-light-border")}`}
        >
          <h2
            className={`text-[0.75em] uppercase tracking-wider font-medium mb-2 px-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Inspector
          </h2>

          {/* Mode toggle — hidden in single-node */}
          {multiNode && (
            <div className="px-1 mb-2">
              <ModeToggle
                mode={mode}
                onChange={(m) => {
                  if (m === "nodes") {
                    onNavigate(encodeParam({ mode: "nodes", nodeId: localNodeId }));
                  } else {
                    onNavigate(encodeParam({ mode: "entities", entityType: "vaults" }));
                  }
                }}
                dark={dark}
              />
            </div>
          )}

          {/* Separator */}
          <div className={`border-t mx-2 mb-1 ${c("border-ink-border-subtle", "border-light-border-subtle")}`} />

          {/* Nav items */}
          {mode === "nodes" ? (
            // Nodes mode: show node list.
            nodes.map((node) => {
              const isActive = selectedNodeId === node.id;
              const isLocal = node.id === localNodeId;
              return (
                <button
                  key={node.id}
                  onClick={() => onNavigate(encodeParam({ mode: "nodes", nodeId: node.id }))}
                  className={`flex items-center gap-2 w-full text-left px-2 py-1.5 rounded text-[0.85em] transition-colors ${
                    isActive
                      ? "bg-copper/15 text-copper"
                      : c(
                          "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                          "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                        )
                  }`}
                >
                  <ClusterIcon className="w-3.5 h-3.5 shrink-0" />
                  <span className="whitespace-nowrap truncate">
                    {node.name || node.id.slice(0, 8)}
                  </span>
                  <span className="ml-auto flex items-center gap-1">
                    {!isLocal && !node.stats && (
                      <Badge variant="error" dark={dark}>offline</Badge>
                    )}
                    {node.role === ClusterNodeRole.LEADER && (
                      <Badge variant="copper" dark={dark}>leader</Badge>
                    )}
                    {node.suffrage === ClusterNodeSuffrage.NONVOTER && (
                      <Badge variant="muted" dark={dark}>nonvoter</Badge>
                    )}
                    {isLocal && (
                      <Badge variant="muted" dark={dark}>this node</Badge>
                    )}
                  </span>
                </button>
              );
            })
          ) : (
            // Entities mode: show entity type list.
            entityNavItems.map(({ id, label, icon: Icon }) => {
              const isActive = parsed.mode === "entities" && parsed.entityType === id;
              const count = entityCounts[id];
              return (
                <button
                  key={id}
                  onClick={() => onNavigate(encodeParam({ mode: "entities", entityType: id }))}
                  className={`flex items-center gap-2 w-full text-left px-2 py-1.5 rounded text-[0.85em] transition-colors ${
                    isActive
                      ? "bg-copper/15 text-copper"
                      : c(
                          "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                          "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                        )
                  }`}
                >
                  <Icon className="w-3.5 h-3.5 shrink-0" />
                  <span className="whitespace-nowrap">{label}</span>
                  {count > 0 && (
                    <span
                      className={`ml-auto text-[0.8em] font-mono ${
                        isActive
                          ? "text-copper/70"
                          : c("text-text-ghost", "text-light-text-ghost")
                      }`}
                    >
                      {count}
                    </span>
                  )}
                </button>
              );
            })
          )}
        </nav>

        {/* ---- Right content pane ---- */}
        <div className="flex-1 overflow-y-auto app-scroll p-5">
          {parsed.mode === "nodes" ? (
            <NodeDetailPane nodeId={selectedNodeId} dark={dark} onOpenSettings={onOpenSettings} />
          ) : (
            <EntityListPane entityType={parsed.entityType} dark={dark} onOpenSettings={onOpenSettings} expandTarget={parsed.expandTarget} />
          )}
        </div>
      </div>
    </Dialog>
  );
}
