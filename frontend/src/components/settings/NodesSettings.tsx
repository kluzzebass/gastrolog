import { useState } from "react";
import { useConfig, useSettings, usePutNodeConfig } from "../../api/hooks/useConfig";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useSetNodeSuffrage } from "../../api/hooks/useSetNodeSuffrage";
import { ClusterNodeRole, ClusterNodeSuffrage } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useEditState } from "../../hooks/useEditState";
import { useToast } from "../Toast";
import { Badge } from "../Badge";
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
      }))
    : localNodeId
      ? [
          {
            id: localNodeId,
            name: settingsData?.nodeName ?? "",
            role: ClusterNodeRole.UNSPECIFIED,
            suffrage: ClusterNodeSuffrage.UNSPECIFIED,
            isLeader: false,
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
    </div>
  );
}
