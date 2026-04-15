import { encode } from "../../api/glid";
import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { useConfig, usePutRoute, useDeleteRoute, useGenerateName } from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import type { SettingsTab } from "./SettingsDialog";
import { sortByName } from "../../lib/sort";

type NavigateTo = (tab: SettingsTab, entityName?: string) => void;

interface DestinationEdit {
  vaultId: string;
}

export function RoutesSettings({ dark, onNavigateTo: _onNavigateTo }: Readonly<{ dark: boolean; onNavigateTo?: NavigateTo }>) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putRoute = usePutRoute();
  const deleteRoute = useDeleteRoute();
  const { addToast } = useToast();
  const generateName = useGenerateName();

  const { isExpanded, toggle: toggleCard } = useExpandedCards();
  const [adding, setAdding] = useState(false);

  const [newName, setNewName] = useState("");
  const [namePlaceholder, setNamePlaceholder] = useState("");
  const [newFilterId, setNewFilterId] = useState("");
  const [newDestinations, setNewDestinations] = useState<DestinationEdit[]>([]);
  const [newDistribution, setNewDistribution] = useState("fanout");
  const [newEnabled, setNewEnabled] = useState(true);
  const [newEjectOnly, setNewEjectOnly] = useState(false);

  const routes = config?.routes ?? [];
  const filters = config?.filters ?? [];
  const vaults = config?.vaults ?? [];
  const existingNames = new Set(routes.map((r) => r.name));
  const effectiveName = newName.trim() || namePlaceholder || "route";
  const nameConflict = existingNames.has(effectiveName);

  const filterOptions = [
    { value: "", label: "(none)" },
    ...filters
      .map((f) => ({ value: encode(f.id), label: f.name || encode(f.id) }))
      .sort((a, b) => a.label.localeCompare(b.label)),
  ];

  const distributionOptions = [
    { value: "fanout", label: "Fanout" },
    { value: "round-robin", label: "Round Robin" },
    { value: "failover", label: "Failover" },
  ];

  const defaults = (id: string) => {
    const route = routes.find((r) => encode(r.id) === id);
    if (!route)
      return {
        name: "",
        filterId: "",
        destinations: [] as DestinationEdit[],
        distribution: "fanout",
        enabled: true,
        ejectOnly: false,
      };
    return {
      name: route.name,
      filterId: encode(route.filterId),
      destinations: route.destinations.map((d) => ({ vaultId: encode(d.vaultId) })),
      distribution: route.distribution || "fanout",
      enabled: route.enabled,
      ejectOnly: route.ejectOnly,
    };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveRoute, handleDelete } = useCrudHandlers({
    mutation: putRoute,
    deleteMutation: deleteRoute,
    label: "Route",
    onSaveTransform: (
      id,
      edit: {
        name: string;
        filterId: string;
        destinations: DestinationEdit[];
        distribution: string;
        enabled: boolean;
        ejectOnly: boolean;
      },
    ) => ({
      id,
      name: edit.name,
      filterId: edit.filterId,
      destinations: edit.destinations.map((d) => d.vaultId),
      distribution: edit.distribution,
      enabled: edit.enabled,
      ejectOnly: edit.ejectOnly,
    }),
    clearEdit,
  });

  const handleSave = (id: string) => saveRoute(id, getEdit(id));

  const handleCreate = async () => {
    const name = newName.trim() || namePlaceholder || "route";
    try {
      await putRoute.mutateAsync({
        id: encode(crypto.getRandomValues(new Uint8Array(16))),
        name,
        filterId: newFilterId,
        destinations: newDestinations.map((d) => d.vaultId),
        distribution: newDistribution,
        enabled: newEnabled,
        ejectOnly: newEjectOnly,
      });
      addToast(`Route "${name}" created`, "info");
      setAdding(false);
      setNewName("");
      setNewFilterId("");
      setNewDestinations([]);
      setNewDistribution("fanout");
      setNewEnabled(true);
      setNewEjectOnly(false);
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create route", "error");
    }
  };

  return (
    <SettingsSection
      addLabel="Add Route"
      adding={adding}
      onToggleAdd={() => {
        if (!adding) {
          generateName.mutateAsync().then(setNamePlaceholder).catch(() => {});
        } else {
          setNamePlaceholder("");
        }
        setNewName("");
        setNewFilterId("");
        setNewDestinations([]);
        setNewDistribution("fanout");
        setNewEnabled(true);
        setNewEjectOnly(false);
        setAdding(!adding);
      }}
      isLoading={isLoading}
      isEmpty={routes.length === 0}
      emptyMessage='No routes configured. Click "Add Route" to create one.'
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => setAdding(false)}
          onCreate={handleCreate}
          isPending={putRoute.isPending}
          createDisabled={nameConflict || newDestinations.length === 0}
        >
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={setNewName}
              placeholder={namePlaceholder || "route"}
              dark={dark}
            />
          </FormField>
          <Checkbox
            checked={newEnabled}
            onChange={setNewEnabled}
            label="Enabled"
            dark={dark}
          />
          <FormField label="Filter" dark={dark}>
            <SelectInput
              value={newFilterId}
              onChange={setNewFilterId}
              options={filterOptions}
              dark={dark}
            />
          </FormField>
          <FormField
            label="Distribution"
            description="Fanout sends to all destinations. Round Robin distributes evenly. Failover sends to the first available destination."
            dark={dark}
          >
            <SelectInput
              value={newDistribution}
              onChange={setNewDistribution}
              options={distributionOptions}
              dark={dark}
            />
          </FormField>
          <DestinationsEditor
            destinations={newDestinations}
            onChange={setNewDestinations}
            vaults={vaults.map((v) => ({ id: encode(v.id), name: v.name }))}
            dark={dark}
          />
          <Checkbox
            checked={newEjectOnly}
            onChange={setNewEjectOnly}
            label="Eject Only"
            dark={dark}
          />
        </AddFormCard>
      )}

      {sortByName(routes).map((route) => {
        const id = encode(route.id);
        const edit = getEdit(id);
        const filterName = filters.find((f) => encode(f.id) === encode(route.filterId))?.name;
        const destNames = route.destinations
          .map((d) => vaults.find((v) => encode(v.id) === encode(d.vaultId))?.name || encode(d.vaultId))
          .join(", ");
        return (
          <SettingsCard
            key={id}
            id={route.name || id}
            dark={dark}
            expanded={isExpanded(id)}
            onToggle={() => toggleCard(id)}
            onDelete={() => handleDelete(id)}
            typeBadge={route.distribution || "fanout"}
            status={
              <span className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
                {filterName || "no filter"}
                {destNames ? ` \u2192 ${destNames}` : ""}
                {!route.enabled && " (disabled)"}
                {route.ejectOnly && " (eject only)"}
              </span>
            }
            footer={
              <Button
                onClick={() => handleSave(id)}
                disabled={putRoute.isPending || !isDirty(id)}
              >
                {putRoute.isPending ? "Saving..." : "Save"}
              </Button>
            }
          >
            <div className="flex flex-col gap-3">
              <FormField label="Name" dark={dark}>
                <TextInput
                  value={edit.name}
                  onChange={(v) => setEdit(id, { name: v })}
                  dark={dark}
                />
              </FormField>
              <Checkbox
                checked={edit.enabled}
                onChange={(v) => setEdit(id, { enabled: v })}
                label="Enabled"
                dark={dark}
              />
              <FormField label="Filter" dark={dark}>
                <SelectInput
                  value={edit.filterId}
                  onChange={(v) => setEdit(id, { filterId: v })}
                  options={filterOptions}
                  dark={dark}
                />
              </FormField>
              <FormField
                label="Distribution"
                description="Fanout sends to all destinations. Round Robin distributes evenly. Failover sends to the first available destination."
                dark={dark}
              >
                <SelectInput
                  value={edit.distribution}
                  onChange={(v) => setEdit(id, { distribution: v })}
                  options={distributionOptions}
                  dark={dark}
                />
              </FormField>
              <DestinationsEditor
                destinations={edit.destinations}
                onChange={(dests) => setEdit(id, { destinations: dests })}
                vaults={vaults.map((v) => ({ id: encode(v.id), name: v.name }))}
                dark={dark}
              />
              <Checkbox
                checked={edit.ejectOnly}
                onChange={(v) => setEdit(id, { ejectOnly: v })}
                label="Eject Only"
                dark={dark}
              />
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}

function DestinationsEditor({
  destinations,
  onChange,
  vaults,
  dark,
}: Readonly<{
  destinations: DestinationEdit[];
  onChange: (dests: DestinationEdit[]) => void;
  vaults: { id: string; name: string }[];
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const usedIds = new Set(destinations.map((d) => d.vaultId));
  const availableVaults = vaults.filter((v) => !usedIds.has(v.id));

  return (
    <FormField label="Destinations" dark={dark}>
      <div className="flex flex-col gap-1.5">
        {destinations.map((dest, idx) => {
          const vault = vaults.find((v) => v.id === dest.vaultId);
          return (
            <div key={dest.vaultId} className="flex items-center gap-2">
              <span
                className={`flex-1 text-[0.85em] px-2.5 py-1.5 border rounded ${c(
                  "bg-ink-surface border-ink-border text-text-bright",
                  "bg-light-surface border-light-border text-light-text-bright",
                )}`}
              >
                {vault?.name || dest.vaultId}
              </span>
              <Button variant="ghost"
                onClick={() => onChange(destinations.filter((_, i) => i !== idx))}
                dark={dark}
              >
                Remove
              </Button>
            </div>
          );
        })}
        {availableVaults.length > 0 && (
          <SelectInput
            value=""
            onChange={(v) => {
              if (v) onChange([...destinations, { vaultId: v }]);
            }}
            options={[
              { value: "", label: "Add destination\u2026" },
              ...availableVaults
                .map((v) => ({ value: v.id, label: v.name || v.id }))
                .sort((a, b) => a.label.localeCompare(b.label)),
            ]}
            dark={dark}
          />
        )}
        {destinations.length === 0 && availableVaults.length === 0 && (
          <p className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            No vaults available
          </p>
        )}
      </div>
    </FormField>
  );
}
