import { useState } from "react";
import { useConfig, useSettings, usePutNodeConfig } from "../../api/hooks/useConfig";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useSetNodeSuffrage } from "../../api/hooks/useSetNodeSuffrage";
import { ClusterNodeRole, ClusterNodeSuffrage } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useEditState } from "../../hooks/useEditState";
import { useToast } from "../Toast";
import { Badge } from "../Badge";
import { CopyButton } from "../CopyButton";
import { EyeIcon, EyeOffIcon } from "../icons";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput } from "./FormField";
import { PrimaryButton, GhostButton } from "./Buttons";

function roleName(role: ClusterNodeRole): string {
  switch (role) {
    case ClusterNodeRole.LEADER:
      return "leader";
    case ClusterNodeRole.FOLLOWER:
      return "follower";
    default:
      return "unknown";
  }
}

interface NodeEdit {
  name: string;
}

export function NodesSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data: configData, isLoading: configLoading } = useConfig();
  const { data: settingsData, isLoading: settingsLoading } = useSettings();
  const { data: clusterData } = useClusterStatus();
  const putNodeConfig = usePutNodeConfig();
  const setNodeSuffrage = useSetNodeSuffrage();
  const { addToast } = useToast();

  const localNodeId = settingsData?.nodeId ?? "";
  const clusterEnabled = clusterData?.clusterEnabled ?? false;

  const nodeConfigMap = new Map(
    (configData?.nodeConfigs ?? []).map((nc) => [nc.id, nc]),
  );

  const nodes = clusterEnabled
    ? (clusterData?.nodes ?? []).map((cn) => ({
        id: cn.id,
        name: nodeConfigMap.get(cn.id)?.name ?? cn.name,
        role: cn.role,
        suffrage: cn.suffrage,
        isLeader: cn.isLeader,
        hasStats: !!cn.stats,
      }))
    : localNodeId
      ? [
          {
            id: localNodeId,
            name: settingsData?.nodeName ?? "",
            role: ClusterNodeRole.UNSPECIFIED,
            suffrage: ClusterNodeSuffrage.UNSPECIFIED,
            isLeader: false,
            hasStats: true,
          },
        ]
      : [];

  const isLeaderNode = clusterEnabled && nodes.some((n) => n.isLeader && n.id === localNodeId);

  const defaults = (id: string): NodeEdit => ({
    name: nodes.find((n) => n.id === id)?.name ?? "",
  });
  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const [expandedCards, setExpandedCards] = useState<Record<string, boolean>>({});
  const toggle = (key: string) =>
    setExpandedCards((prev) => ({ ...prev, [key]: !prev[key] }));

  const handleSave = async (nodeId: string) => {
    const edit = getEdit(nodeId);
    if (!edit.name.trim()) {
      addToast("Node name must not be empty", "error");
      return;
    }
    try {
      await putNodeConfig.mutateAsync({ id: nodeId, name: edit.name.trim() });
      clearEdit(nodeId);
      addToast("Node name updated", "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update node name", "error");
    }
  };

  const isLoading = configLoading || settingsLoading;

  if (isLoading) {
    return (
      <div className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
        Loading...
      </div>
    );
  }

  return (
    <div>
      <div className="flex flex-col gap-3">
        {nodes.toSorted((a, b) => a.name.localeCompare(b.name)).map((node) => {
          const isLocal = node.id === localNodeId;
          const displayName = getEdit(node.id).name || node.name || "Unnamed Node";
          const dirty = isDirty(node.id);

          return (
            <SettingsCard
              key={node.id}
              id={displayName}
              dark={dark}
              expanded={expandedCards[node.id] ?? isLocal}
              onToggle={() => toggle(node.id)}
              headerRight={
                <div className="flex items-center gap-1.5">
                  {clusterEnabled && !isLocal && !node.hasStats && (
                    <Badge variant="error" dark={dark}>offline</Badge>
                  )}
                  {clusterEnabled && node.role !== ClusterNodeRole.UNSPECIFIED && (
                    <Badge variant={node.isLeader ? "copper" : "muted"} dark={dark}>
                      {roleName(node.role)}
                    </Badge>
                  )}
                  {clusterEnabled && node.suffrage === ClusterNodeSuffrage.NONVOTER && (
                    <Badge variant="muted" dark={dark}>nonvoter</Badge>
                  )}
                  {isLocal && (
                    <Badge variant="muted" dark={dark}>this node</Badge>
                  )}
                </div>
              }
              footer={
                dirty ? (
                  <>
                    <GhostButton onClick={() => clearEdit(node.id)} dark={dark}>
                      Reset
                    </GhostButton>
                    <PrimaryButton
                      onClick={() => handleSave(node.id)}
                      disabled={putNodeConfig.isPending}
                    >
                      {putNodeConfig.isPending ? "Saving..." : "Save"}
                    </PrimaryButton>
                  </>
                ) : undefined
              }
            >
              <FormField label="Node Name" dark={dark}>
                <TextInput
                  value={getEdit(node.id).name}
                  onChange={(name) => setEdit(node.id, { name })}
                  placeholder="e.g. us-east-1"
                  dark={dark}
                  mono
                />
              </FormField>
              {clusterEnabled && !node.isLeader && isLeaderNode && (
                <div className="pt-1">
                  {node.suffrage === ClusterNodeSuffrage.VOTER ? (
                    <GhostButton
                      onClick={async () => {
                        try {
                          await setNodeSuffrage.mutateAsync({ nodeId: node.id, voter: false });
                          addToast("Demoted to nonvoter", "info");
                        } catch (err: any) {
                          addToast(err.message ?? "Failed to demote", "error");
                        }
                      }}
                      dark={dark}
                    >
                      Demote to Nonvoter
                    </GhostButton>
                  ) : node.suffrage === ClusterNodeSuffrage.NONVOTER ? (
                    <GhostButton
                      onClick={async () => {
                        try {
                          await setNodeSuffrage.mutateAsync({ nodeId: node.id, voter: true });
                          addToast("Promoted to voter", "info");
                        } catch (err: any) {
                          addToast(err.message ?? "Failed to promote", "error");
                        }
                      }}
                      dark={dark}
                    >
                      Promote to Voter
                    </GhostButton>
                  ) : null}
                </div>
              )}
            </SettingsCard>
          );
        })}

        {nodes.length === 0 && (
          <div
            className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            No nodes found.
          </div>
        )}
      </div>

      {clusterEnabled && clusterData?.joinToken && (
        <JoinInfoCard dark={dark} joinToken={clusterData.joinToken} clusterAddress={clusterData.clusterAddress} />
      )}
    </div>
  );
}

function JoinInfoCard({ dark, joinToken, clusterAddress }: Readonly<{ dark: boolean; joinToken: string; clusterAddress: string }>) {
  const c = useThemeClass(dark);
  const [showToken, setShowToken] = useState(false);

  const maskedToken = joinToken.slice(0, 8) + "..." + joinToken.slice(-4);
  const displayToken = showToken ? joinToken : maskedToken;

  const joinCmd = `gastrolog server --join-addr ${clusterAddress || "<cluster-addr>"} --join-token ${joinToken} --cluster-addr :4575`;

  return (
    <div className={`mt-4 rounded-lg border p-4 ${c(
      "bg-ink-well/50 border-ink-border",
      "bg-light-well/50 border-light-border",
    )}`}>
      <h3 className={`text-[0.85em] font-semibold mb-3 ${c("text-text-primary", "text-light-text-primary")}`}>
        Cluster Join Info
      </h3>
      <div className="flex flex-col gap-2.5">
        <div className="flex flex-col gap-1">
          <span className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}>
            Cluster Address
          </span>
          <div className="flex items-center gap-1.5">
            <code className={`text-[0.8em] font-mono ${c("text-text-secondary", "text-light-text-secondary")}`}>
              {clusterAddress || "—"}
            </code>
            {clusterAddress && <CopyButton text={clusterAddress} dark={dark} />}
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <span className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}>
            Join Token
          </span>
          <div className="flex items-center gap-1.5">
            <code className={`text-[0.8em] font-mono break-all ${c("text-text-secondary", "text-light-text-secondary")}`}>
              {displayToken}
            </code>
            <button
              type="button"
              onClick={() => setShowToken(!showToken)}
              className={`shrink-0 transition-colors ${c("text-text-ghost hover:text-copper", "text-light-text-ghost hover:text-copper")}`}
              title={showToken ? "Hide token" : "Reveal token"}
            >
              {showToken ? <EyeOffIcon className="w-3.5 h-3.5" /> : <EyeIcon className="w-3.5 h-3.5" />}
            </button>
            <CopyButton text={joinToken} dark={dark} />
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <span className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}>
            Join Command
          </span>
          <div className="flex items-start gap-1.5">
            <code className={`text-[0.75em] font-mono break-all leading-relaxed ${c(
              "text-text-ghost bg-ink-well px-2 py-1.5 rounded",
              "text-light-text-ghost bg-light-well px-2 py-1.5 rounded",
            )}`}>
              {joinCmd}
            </code>
            <CopyButton text={joinCmd} dark={dark} className="mt-1 shrink-0" />
          </div>
        </div>
      </div>
    </div>
  );
}
