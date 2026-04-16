import { encode } from "../../api/glid";
import { useState } from "react";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { useConfig, usePutNodeConfig } from "../../api/hooks/useSystem";
import { useSettings } from "../../api/hooks/useSettings";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useSetNodeSuffrage } from "../../api/hooks/useSetNodeSuffrage";
import { useJoinCluster } from "../../api/hooks/useJoinCluster";
import { useRemoveNode } from "../../api/hooks/useRemoveNode";
import { ClusterNodeRole, ClusterNodeSuffrage } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useEditState } from "../../hooks/useEditState";
import { useToast } from "../Toast";
import { Badge } from "../Badge";
import { CopyButton } from "../CopyButton";
import { EyeIcon, EyeOffIcon } from "../icons";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput } from "./FormField";
import { Button } from "./Buttons";
import { sortByName } from "../../lib/sort";

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
  const removeNode = useRemoveNode();
  const { addToast } = useToast();

  const localNodeId = settingsData?.nodeId ? encode(settingsData.nodeId) : "";
  const clusterEnabled = clusterData?.clusterEnabled ?? false;

  const nodeConfigMap = new Map(
    (configData?.nodeConfigs ?? []).map((nc) => [encode(nc.id), nc]),
  );

  let nodes: { id: string; name: string; role: ClusterNodeRole; suffrage: ClusterNodeSuffrage; isLeader: boolean; hasStats: boolean }[];
  if (clusterEnabled) {
    nodes = (clusterData?.nodes ?? []).map((cn) => ({
      id: encode(cn.id),
      name: nodeConfigMap.get(encode(cn.id))?.name ?? cn.name,
      role: cn.role,
      suffrage: cn.suffrage,
      isLeader: cn.isLeader,
      hasStats: !!cn.stats,
    }));
  } else if (localNodeId) {
    nodes = [{
      id: localNodeId,
      name: settingsData?.nodeName ?? "",
      role: ClusterNodeRole.UNSPECIFIED,
      suffrage: ClusterNodeSuffrage.UNSPECIFIED,
      isLeader: false,
      hasStats: true,
    }];
  } else {
    nodes = [];
  }

  const voterCount = nodes.filter((n) => n.suffrage === ClusterNodeSuffrage.VOTER).length;

  const defaults = (id: string): NodeEdit => ({
    name: nodes.find((n) => n.id === id)?.name ?? "",
  });
  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { expandedCards, toggle, isExpanded } = useExpandedCards();
  const [confirmRemoveId, setConfirmRemoveId] = useState<string | null>(null);

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
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to update node name", "error");
    }
  };

  const isLoading = configLoading || settingsLoading;

  if (isLoading) {
    return <LoadingPlaceholder dark={dark} />;
  }

  return (
    <div>
      <div className="flex flex-col gap-3">
        {sortByName(nodes).map((node) => {
          const isLocal = node.id === localNodeId;
          const displayName = getEdit(node.id).name || node.name || "Unnamed Node";
          const dirty = isDirty(node.id);

          return (
            <SettingsCard
              key={node.id}
              id={displayName}
              dark={dark}
              expanded={node.id in expandedCards ? isExpanded(node.id) : isLocal}
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
                    <Button variant="ghost" onClick={() => clearEdit(node.id)} dark={dark}>
                      Discard
                    </Button>
                    <Button
                      onClick={() => handleSave(node.id)}
                      disabled={putNodeConfig.isPending}
                    >
                      {putNodeConfig.isPending ? "Saving..." : "Save"}
                    </Button>
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
              {clusterEnabled && (
                <div className="pt-1 flex items-center gap-2">
                  {node.suffrage === ClusterNodeSuffrage.VOTER && (
                    <Button variant="ghost"
                      onClick={async () => {
                        try {
                          await setNodeSuffrage.mutateAsync({ nodeId: node.id, voter: false });
                          addToast("Demoted to nonvoter", "info");
                        } catch (err: unknown) {
                          addToast(err instanceof Error ? err.message : "Failed to demote", "error");
                        }
                      }}
                      dark={dark}
                      bordered
                      disabled={voterCount <= 1}
                    >
                      Demote
                    </Button>
                  )}
                  {node.suffrage === ClusterNodeSuffrage.NONVOTER && (
                    <Button variant="ghost"
                      onClick={async () => {
                        try {
                          await setNodeSuffrage.mutateAsync({ nodeId: node.id, voter: true });
                          addToast("Promoted to voter", "info");
                        } catch (err: unknown) {
                          addToast(err instanceof Error ? err.message : "Failed to promote", "error");
                        }
                      }}
                      dark={dark}
                      bordered
                    >
                      Promote
                    </Button>
                  )}
                  {!isLocal && (confirmRemoveId === node.id ? (
                    <>
                      <span className={`text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}>
                        This will evict the node from the cluster.
                      </span>
                      <Button variant="danger"
                        onClick={async () => {
                          try {
                            await removeNode.mutateAsync({ nodeId: node.id });
                            addToast("Node removed from cluster", "info");
                            setConfirmRemoveId(null);
                          } catch (err: unknown) {
                            addToast(err instanceof Error ? err.message : "Failed to remove node", "error");
                          }
                        }}
                        disabled={removeNode.isPending}
                      >
                        {removeNode.isPending ? "Removing..." : "Remove"}
                      </Button>
                      <Button variant="ghost" onClick={() => setConfirmRemoveId(null)} dark={dark}>
                        Cancel
                      </Button>
                    </>
                  ) : (
                    <Button variant="danger" onClick={() => setConfirmRemoveId(node.id)}>
                      Remove
                    </Button>
                  ))}
                </div>
              )}
            </SettingsCard>
          );
        })}

        {nodes.length === 0 && (
          <div
            className={`text-center py-8 text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            No nodes found.
          </div>
        )}
      </div>

      {clusterEnabled && clusterData?.joinToken && (
        <JoinInfoCard dark={dark} joinToken={clusterData.joinToken} clusterAddress={clusterData.clusterAddress} />
      )}

      {clusterEnabled && nodes.length === 1 && (
        <JoinClusterCard dark={dark} />
      )}
    </div>
  );
}

function JoinClusterCard({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { addToast } = useToast();
  const joinCluster = useJoinCluster();
  const [leaderAddress, setLeaderAddress] = useState("");
  const [joinToken, setJoinToken] = useState("");
  const [confirmed, setConfirmed] = useState(false);

  const canSubmit = leaderAddress.trim() !== "" && joinToken.trim() !== "" && confirmed;

  const handleJoin = async () => {
    try {
      await joinCluster.mutateAsync({
        leaderAddress: leaderAddress.trim(),
        joinToken: joinToken.trim(),
      });
      addToast("Successfully joined cluster", "info");
      setLeaderAddress("");
      setJoinToken("");
      setConfirmed(false);
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to join cluster", "error");
    }
  };

  return (
    <div className={`mt-4 rounded-lg border p-4 ${c(
      "bg-ink-well/50 border-ink-border",
      "bg-light-well/50 border-light-border",
    )}`}>
      <h3 className={`text-[0.85em] font-semibold mb-1 ${c("text-text-bright", "text-light-text-bright")}`}>
        Join Cluster
      </h3>
      <p className={`text-[0.75em] mb-3 ${c("text-text-muted", "text-light-text-muted")}`}>
        Join an existing cluster at runtime. This node's local configuration will be replaced by the cluster's configuration.
      </p>
      <div className="flex flex-col gap-2.5">
        <FormField label="Leader Address" dark={dark}>
          <TextInput
            value={leaderAddress}
            onChange={setLeaderAddress}
            placeholder="e.g. 10.0.0.1:4566"
            dark={dark}
            mono
            disabled={joinCluster.isPending}
          />
        </FormField>
        <FormField label="Join Token" dark={dark}>
          <TextInput
            value={joinToken}
            onChange={setJoinToken}
            placeholder="Paste join token from the leader"
            dark={dark}
            mono
            disabled={joinCluster.isPending}
          />
        </FormField>
        <label className={`flex items-start gap-2 text-[0.75em] cursor-pointer ${c("text-text-muted", "text-light-text-muted")}`}>
          <input
            type="checkbox"
            checked={confirmed}
            onChange={(e) => setConfirmed(e.target.checked)}
            disabled={joinCluster.isPending}
            className="mt-0.5"
          />
          <span>
            I understand that this node's local config will be replaced by the remote cluster's config. This action cannot be undone.
          </span>
        </label>
        <div className="flex justify-end pt-1">
          <Button
            onClick={handleJoin}
            disabled={!canSubmit || joinCluster.isPending}
          >
            {joinCluster.isPending ? "Joining..." : "Join Cluster"}
          </Button>
        </div>
      </div>
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
      <h3 className={`text-[0.85em] font-semibold mb-3 ${c("text-text-bright", "text-light-text-bright")}`}>
        Cluster Join Info
      </h3>
      <div className="flex flex-col gap-2.5">
        <div className="flex flex-col gap-1">
          <span className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}>
            Cluster Address
          </span>
          <div className="flex items-center gap-1.5">
            <code className={`text-[0.8em] font-mono ${c("text-text-normal", "text-light-text-normal")}`}>
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
            <code className={`text-[0.8em] font-mono break-all ${c("text-text-normal", "text-light-text-normal")}`}>
              {displayToken}
            </code>
            <button
              type="button"
              onClick={() => setShowToken(!showToken)}
              className={`shrink-0 transition-colors ${c("text-text-muted hover:text-copper", "text-light-text-muted hover:text-copper")}`}
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
              "text-text-muted bg-ink-well px-2 py-1.5 rounded",
              "text-light-text-muted bg-light-well px-2 py-1.5 rounded",
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
