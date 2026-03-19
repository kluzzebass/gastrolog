import { useState, useReducer } from "react";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import {
  useConfig,
  usePutVault,
  useGenerateName,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { VaultParamsForm } from "./VaultParamsForm";
import { NodeSelect } from "./NodeSelect";
import { sortByName } from "../../lib/sort";
import { RetentionRuleEditor, retentionRulesValid } from "./VaultHelpers";
import type { RetentionRuleEdit } from "./VaultHelpers";
import { VaultSettingsCard } from "./VaultSettingsCard";

interface AddFormState {
  adding: boolean;
  name: string;
  namePlaceholder: string;
  type: string;
  policy: string;
  retentionRules: RetentionRuleEdit[];
  params: Record<string, string>;
  nodeId: string;
}

const addFormInitial: AddFormState = {
  adding: false,
  name: "",
  namePlaceholder: "",
  type: "memory",
  policy: "",
  retentionRules: [],
  params: {},
  nodeId: "",
};

type AddFormAction =
  | { type: "open"; vaultType: string }
  | { type: "close" }
  | { type: "reset" }
  | { type: "set"; patch: Partial<AddFormState> };

function addFormReducer(state: AddFormState, action: AddFormAction): AddFormState {
  switch (action.type) {
    case "open":
      return { ...addFormInitial, adding: true, type: action.vaultType };
    case "close":
    case "reset":
      return addFormInitial;
    case "set":
      return { ...state, ...action.patch };
  }
}

export function VaultsSettings({ dark, expandTarget, onExpandTargetConsumed, onOpenInspector }: Readonly<{ dark: boolean; expandTarget?: string | null; onExpandTargetConsumed?: () => void; onOpenInspector?: (inspectorParam: string) => void }>) {
  const { data: config, isLoading } = useConfig();
  const putVault = usePutVault();
  const { addToast } = useToast();

  const { isExpanded, toggle: toggleCard, setExpandedCards } = useExpandedCards();
  const generateName = useGenerateName();

  const [addForm, dispatchAdd] = useReducer(addFormReducer, addFormInitial);

  const configVaults = config?.vaults;
  const vaults = configVaults ?? [];
  const existingNames = new Set(vaults.map((s) => s.name));
  const effectiveName = addForm.name.trim() || addForm.namePlaceholder || addForm.type;
  const nameConflict = existingNames.has(effectiveName);
  const policies = config?.rotationPolicies ?? [];
  const retentionPolicies = config?.retentionPolicies ?? [];
  const routes = config?.routes ?? [];

  // Auto-expand a vault when navigated to from another view.
  const [consumedExpandTarget, setConsumedExpandTarget] = useState<string | null>(null);
  if (expandTarget && expandTarget !== consumedExpandTarget && configVaults && configVaults.length > 0) {
    setConsumedExpandTarget(expandTarget);
    const match = configVaults.find((s) => (s.name || s.id) === expandTarget);
    if (match) {
      setExpandedCards((prev) => ({ ...prev, [match.id]: true }));
    }
    onExpandTargetConsumed?.();
  }

  const policyOptions = [
    { value: "", label: "(none)" },
    ...policies
      .map((p) => ({ value: p.id, label: p.name || p.id }))
      .sort((a, b) => a.label.localeCompare(b.label)),
  ];

  const handleCreate = async () => {
    const name = addForm.name.trim() || addForm.namePlaceholder || addForm.type;
    try {
      await putVault.mutateAsync({
        id: "",
        name,
        type: addForm.type,
        policy: addForm.policy,
        retentionRules: addForm.retentionRules,
        params: addForm.params,
        nodeId: addForm.nodeId,
      });
      addToast(`Vault "${name}" created`, "info");
      dispatchAdd({ type: "reset" });
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create vault", "error");
    }
  };

  return (
    <SettingsSection
      addLabel="Add Vault"
      adding={addForm.adding}
      onToggleAdd={() => dispatchAdd({ type: "close" })}
      addOptions={[
        { value: "memory", label: "memory" },
        { value: "file", label: "file" },
      ]}
      onAddSelect={(type) => {
        dispatchAdd({ type: "open", vaultType: type });
        generateName.mutateAsync().then((n) => dispatchAdd({ type: "set", patch: { namePlaceholder: n } }));
      }}
      isLoading={isLoading}
      isEmpty={vaults.length === 0}
      emptyMessage='No vaults configured. Click "Add Vault" to create one.'
      dark={dark}
    >
      {addForm.adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => dispatchAdd({ type: "close" })}
          onCreate={handleCreate}
          isPending={putVault.isPending}
          createDisabled={nameConflict || !retentionRulesValid(addForm.retentionRules)}
          typeBadge={addForm.type}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={addForm.name}
              onChange={(v) => dispatchAdd({ type: "set", patch: { name: v } })}
              placeholder={addForm.namePlaceholder || addForm.type}
              dark={dark}
            />
          </FormField>
          <NodeSelect value={addForm.nodeId} onChange={(v) => dispatchAdd({ type: "set", patch: { nodeId: v } })} dark={dark} />
          <FormField label="Rotation Policy" dark={dark}>
            <SelectInput
              value={addForm.policy}
              onChange={(v) => dispatchAdd({ type: "set", patch: { policy: v } })}
              options={policyOptions}
              dark={dark}
            />
          </FormField>
          <RetentionRuleEditor
            rules={addForm.retentionRules}
            onChange={(rules) => dispatchAdd({ type: "set", patch: { retentionRules: rules } })}
            retentionPolicies={retentionPolicies}
            routes={routes}
            dark={dark}
          />
          <VaultParamsForm
            vaultType={addForm.type}
            params={addForm.params}
            onChange={(p) => dispatchAdd({ type: "set", patch: { params: p } })}
            dark={dark}
            vaultName={effectiveName}
          />
        </AddFormCard>
      )}

      {sortByName(vaults).map((vault) => (
        <VaultSettingsCard
          key={vault.id}
          vault={vault}
          vaults={vaults}
          routes={routes}
          policies={policies}
          retentionPolicies={retentionPolicies}
          dark={dark}
          expanded={isExpanded(vault.id)}
          onToggle={() => toggleCard(vault.id)}
          onOpenInspector={onOpenInspector}
        />
      ))}
    </SettingsSection>
  );
}
