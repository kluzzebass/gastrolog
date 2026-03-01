import { useState } from "react";
import { useConfig, useSettings, usePutNodeConfig } from "../../api/hooks/useConfig";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { ClusterNodeRole } from "../../api/gen/gastrolog/v1/lifecycle_pb";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useEditState } from "../../hooks/useEditState";
import { useToast } from "../Toast";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput } from "./FormField";
import { PrimaryButton, GhostButton } from "./Buttons";

function roleName(role: ClusterNodeRole): string {
  switch (role) {
    case ClusterNodeRole.LEADER:
      return "Leader";
    case ClusterNodeRole.FOLLOWER:
      return "Follower";
    default:
      return "Unknown";
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
        isLeader: cn.isLeader,
      }))
    : localNodeId
      ? [
          {
            id: localNodeId,
            name: settingsData?.nodeName ?? "",
            role: ClusterNodeRole.UNSPECIFIED,
            isLeader: false,
          },
        ]
      : [];

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
      <div className="flex items-center justify-between mb-5">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Nodes
        </h2>
      </div>

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
                    <span
                      className={`px-1.5 py-0.5 text-[0.7em] font-medium uppercase tracking-wider rounded ${
                        node.isLeader
                          ? "bg-copper/15 text-copper"
                          : c("bg-ink-hover text-text-muted", "bg-light-hover text-light-text-muted")
                      }`}
                    >
                      {roleName(node.role)}
                    </span>
                  )}
                  {isLocal && (
                    <span
                      className={`px-1.5 py-0.5 text-[0.7em] font-medium uppercase tracking-wider rounded ${c(
                        "bg-ink-hover text-text-muted",
                        "bg-light-hover text-light-text-muted",
                      )}`}
                    >
                      this node
                    </span>
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
