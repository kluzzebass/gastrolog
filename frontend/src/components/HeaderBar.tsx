import { useState, useRef, useEffect } from "react";
import { StatPill } from "./StatPill";
import { UserMenu } from "./UserMenu";
import { AlertPanel } from "./AlertPanel";
import { SlidersIcon } from "./icons";
import { useThemeClass } from "../hooks/useThemeClass";
import { useClusterStatus } from "../api/hooks/useClusterStatus";
import { useAlerts } from "../api/hooks/useAlerts";
import { AlertSeverity } from "../api/gen/gastrolog/v1/cluster_pb";
import { formatBytes } from "../utils/units";
import type { ClusterNode } from "../api/gen/gastrolog/v1/lifecycle_pb";

interface HeaderBarProps {
  dark: boolean;
  onShowHelp: () => void;
  onShowInspector: () => void;
  onShowSettings: () => void;
  currentUser: { username: string; role: string } | null;
  onPreferences: () => void;
  onChangePassword: () => void;
  onLogout: () => void;
}

export function HeaderBar({
  dark,
  onShowHelp,
  onShowInspector,
  onShowSettings,
  currentUser,
  onPreferences,
  onChangePassword,
  onLogout,
}: Readonly<HeaderBarProps>) {
  const c = useThemeClass(dark);
  const { data: cluster, isLoading } = useClusterStatus();
  const nodes = (cluster?.nodes ?? []).toSorted((a, b) =>
    (a.name || a.id).localeCompare(b.name || b.id),
  );

  // Inspector glow: briefly flash when system status data arrives.
  // Triggered by cluster data changes (pushed via WatchSystemStatus stream).
  const [inspectorGlow, setInspectorGlow] = useState(false);
  const glowTimer = useRef<ReturnType<typeof setTimeout>>(null);
  const prevClusterRef = useRef(cluster);
  useEffect(() => {
    if (cluster && cluster !== prevClusterRef.current) {
      prevClusterRef.current = cluster;
      setInspectorGlow(true);
      if (glowTimer.current) clearTimeout(glowTimer.current);
      glowTimer.current = setTimeout(() => setInspectorGlow(false), 800);
    }
  }, [cluster]);

  // Aggregate stats across all nodes.
  let totalCpu = 0;
  let totalMemory = 0;
  let totalStorage = 0;
  for (const node of nodes) {
    const s = node.stats;
    if (!s) continue;
    totalCpu += s.cpuPercent;
    totalMemory += Number(s.memoryRss);
    for (const v of s.vaults) {
      totalStorage += Number(v.dataBytes);
    }
  }

  const { alerts, maxSeverity } = useAlerts();
  const [alertPanelOpen, setAlertPanelOpen] = useState(false);

  const loading = isLoading || nodes.length === 0;
  const noQuorum = !isLoading && cluster?.clusterEnabled && nodes.length > 1 && !cluster.leaderId;

  return (
    <header
      className={`flex items-center justify-between px-4 lg:px-7 py-3.5 border-b ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`}
    >
      <div className="flex items-center gap-3">
        <img src="/favicon.svg" alt="GastroLog" className="w-6 h-6" />
        <h1
          className={`font-display text-[1.6em] font-semibold tracking-tight leading-none ${c("text-text-bright", "text-light-text-bright")}`}
        >
          GastroLog
        </h1>
        {noQuorum && (
          <span
            className="px-2.5 py-1 text-[0.7em] font-mono font-semibold rounded bg-severity-warn/15 text-severity-warn"
            title="Cluster has no leader — configuration changes are blocked. Searches continue working."
          >
            No Quorum
          </span>
        )}
        {alerts.length > 0 && (
          <button
            onClick={() => setAlertPanelOpen(true)}
            className={`flex items-center gap-1.5 px-2.5 py-1 text-[0.7em] font-mono font-semibold rounded transition-all duration-200 ${
              maxSeverity === AlertSeverity.ERROR
                ? "bg-severity-error/15 text-severity-error"
                : "bg-severity-warn/15 text-severity-warn"
            }`}
            title={`${alerts.length} active alert${alerts.length === 1 ? "" : "s"}`}
          >
            <span
              className={`inline-block w-2 h-2 rounded-full animate-pulse ${
                maxSeverity === AlertSeverity.ERROR ? "bg-severity-error" : "bg-severity-warn"
              }`}
            />
            {alerts.length} {alerts.length === 1 ? "Alert" : "Alerts"}
          </button>
        )}
      </div>

      <div className="flex items-center gap-3 lg:gap-6">
        {/* Stats ribbon */}
        <div className="hidden lg:flex items-center gap-5">
          <HoverStat
            label="CPU"
            value={loading ? "..." : `${totalCpu.toFixed(1)}%`}
            dark={dark}
            nodes={nodes}
            renderNodeValue={(n) => `${n.stats?.cpuPercent.toFixed(1) ?? "—"}%`}
          />
          <span className={`text-xs ${c("text-ink-border", "text-light-border")}`}>|</span>
          <HoverStat
            label="Memory"
            value={loading ? "..." : formatBytes(totalMemory)}
            dark={dark}
            nodes={nodes}
            renderNodeValue={(n) => formatBytes(Number(n.stats?.memoryRss ?? 0))}
          />
          <span className={`text-xs ${c("text-ink-border", "text-light-border")}`}>|</span>
          <HoverStat
            label="Storage"
            value={loading ? "..." : formatBytes(totalStorage)}
            dark={dark}
            nodes={nodes}
            renderNodeValue={(n) => {
              let bytes = 0;
              for (const v of n.stats?.vaults ?? []) bytes += Number(v.dataBytes);
              return formatBytes(bytes);
            }}
          />
        </div>

        <button
          onClick={onShowHelp}
          aria-label="Help"
          title="Help"
          className={`w-9 h-9 flex items-center justify-center rounded transition-all duration-200 ${c(
            "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )}`}
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="w-4 h-4"
          >
            <circle cx="12" cy="12" r="10" />
            <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
        </button>

        <button
          onClick={onShowInspector}
          aria-label="Inspector"
          title="Inspector"
          className={`w-9 h-9 flex items-center justify-center rounded transition-all duration-500 ${c(
            "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )} ${inspectorGlow ? "text-copper drop-shadow-[0_0_4px_var(--color-copper)]" : ""}`}
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="w-4 h-4"
          >
            <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
          </svg>
        </button>

        <button
          onClick={onShowSettings}
          aria-label="Settings"
          title="Settings"
          className={`w-9 h-9 flex items-center justify-center rounded transition-all duration-200 ${c(
            "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )}`}
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="w-4 h-4"
          >
            <circle cx="12" cy="12" r="3" />
            <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
          </svg>
        </button>

        {currentUser ? (
          <UserMenu
            username={currentUser.username}
            role={currentUser.role}
            dark={dark}
            onPreferences={onPreferences}
            onChangePassword={onChangePassword}
            onLogout={onLogout}
          />
        ) : (
          <button
            onClick={onPreferences}
            aria-label="Preferences"
            title="Preferences"
            className={`w-9 h-9 flex items-center justify-center rounded transition-all duration-200 ${c(
              "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
              "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
            )}`}
          >
            <SlidersIcon className="w-4 h-4" />
          </button>
        )}
      </div>
      {alertPanelOpen && (
        <AlertPanel alerts={alerts} dark={dark} onClose={() => setAlertPanelOpen(false)} />
      )}
    </header>
  );
}

// ---- Hoverable stat with per-node tooltip ----

function HoverStat({
  label,
  value,
  dark,
  nodes,
  renderNodeValue,
}: Readonly<{
  label: string;
  value: string;
  dark: boolean;
  nodes: ClusterNode[];
  renderNodeValue: (node: ClusterNode) => string;
}>) {
  const [hover, setHover] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>(null);
  const c = useThemeClass(dark);

  const showTooltip = () => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    setHover(true);
  };

  const hideTooltip = () => {
    timeoutRef.current = setTimeout(() => setHover(false), 150);
  };

  // Only show tooltip in multi-node mode.
  const multiNode = nodes.length > 1;

  return (
    <div
      className="relative"
      onMouseEnter={multiNode ? showTooltip : undefined}
      onMouseLeave={multiNode ? hideTooltip : undefined}
    >
      <StatPill label={label} value={value} dark={dark} />
      {hover && multiNode && (
        <div
          className={`absolute top-full right-0 mt-2 z-50 rounded-lg border shadow-lg py-2 px-3 min-w-40 ${c(
            "bg-ink-raised border-ink-border-subtle",
            "bg-light-raised border-light-border-subtle",
          )}`}
          onMouseEnter={showTooltip}
          onMouseLeave={hideTooltip}
        >
          {nodes.map((node) => (
            <div key={node.id} className="flex items-baseline justify-between gap-4 py-0.5">
              <span className={`text-[0.75em] truncate max-w-24 ${c("text-text-muted", "text-light-text-muted")}`}>
                {node.name || node.id.slice(0, 8)}
              </span>
              <span className={`text-[0.75em] font-mono shrink-0 ${c("text-text-bright", "text-light-text-bright")}`}>
                {node.stats ? renderNodeValue(node) : "—"}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
