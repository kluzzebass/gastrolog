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
import { FormField, TextInput } from "./FormField";
import { sortByName } from "../../lib/sort";
import { VaultSettingsCard } from "./VaultSettingsCard";

interface AddFormState {
  adding: boolean;
  name: string;
  namePlaceholder: string;
}

const addFormInitial: AddFormState = {
  adding: false,
  name: "",
  namePlaceholder: "",
};

type AddFormAction =
  | { type: "open" }
  | { type: "close" }
  | { type: "reset" }
  | { type: "set"; patch: Partial<AddFormState> };

function addFormReducer(state: AddFormState, action: AddFormAction): AddFormState {
  switch (action.type) {
    case "open":
      return { ...addFormInitial, adding: true };
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
  const effectiveName = addForm.name.trim() || addForm.namePlaceholder || "vault";
  const nameConflict = existingNames.has(effectiveName);
  const tiers = config?.tiers ?? [];
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

  const handleCreate = async () => {
    const name = addForm.name.trim() || addForm.namePlaceholder || "vault";
    try {
      await putVault.mutateAsync({
        id: "",
        name,
        tierIds: [],
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
      onToggleAdd={() => {
        if (!addForm.adding) {
          dispatchAdd({ type: "open" });
          generateName.mutateAsync().then((n) => dispatchAdd({ type: "set", patch: { namePlaceholder: n } }));
        } else {
          dispatchAdd({ type: "close" });
        }
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
          createDisabled={nameConflict}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={addForm.name}
              onChange={(v) => dispatchAdd({ type: "set", patch: { name: v } })}
              placeholder={addForm.namePlaceholder || "vault"}
              dark={dark}
            />
          </FormField>
        </AddFormCard>
      )}

      {sortByName(vaults).map((vault) => (
        <VaultSettingsCard
          key={vault.id}
          vault={vault}
          vaults={vaults}
          tiers={tiers}
          routes={routes}
          dark={dark}
          expanded={isExpanded(vault.id)}
          onToggle={() => toggleCard(vault.id)}
          onOpenInspector={onOpenInspector}
        />
      ))}
    </SettingsSection>
  );
}
