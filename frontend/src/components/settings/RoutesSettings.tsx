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
import type { RouteConfig } from "../../api/gen/gastrolog/v1/system_pb";

// gastrolog-4kkoo (Phase 5): the route's gating predicate is the inline
// expression on the first MatchStage. Future stage kinds (gastrolog-5e85x:
// enrich, redact, sample, fork, route_by_field) plug into the same oneof.
function routeExpression(r: RouteConfig): string {
  for (const stage of r.stages) {
    if (stage.stage.case === "match") return stage.stage.value.expression;
  }
  return "";
}

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
  const [newPriority, setNewPriority] = useState(0);
  const [newExpression, setNewExpression] = useState("*");
  const [newDestinations, setNewDestinations] = useState<DestinationEdit[]>([]);
  const [newDistribution, setNewDistribution] = useState("fanout");
  const [newEnabled, setNewEnabled] = useState(true);

  const routes = config?.routes ?? [];
  const vaults = config?.vaults ?? [];
  const existingNames = new Set(routes.map((r) => r.name));
  const effectiveName = newName.trim() || namePlaceholder || "route";
  const nameConflict = existingNames.has(effectiveName);

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
        priority: 0,
        expression: "*",
        destinations: [] as DestinationEdit[],
        distribution: "fanout",
        enabled: true,
      };
    return {
      name: route.name,
      priority: route.priority,
      expression: routeExpression(route),
      destinations: route.destinations.map((d) => ({ vaultId: encode(d.vaultId) })),
      distribution: route.distribution || "fanout",
      enabled: route.enabled,
    };
  };

  const { getEdit, setEdit, clearEdit: _clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveRoute, handleDelete } = useCrudHandlers({
    mutation: putRoute,
    deleteMutation: deleteRoute,
    label: "Route",
    onSaveTransform: (
      id,
      edit: {
        name: string;
        priority: number;
        expression: string;
        destinations: DestinationEdit[];
        distribution: string;
        enabled: boolean;
      },
    ) => ({
      id,
      name: edit.name,
      priority: edit.priority,
      expression: edit.expression,
      destinations: edit.destinations.map((d) => d.vaultId),
      distribution: edit.distribution,
      enabled: edit.enabled,
    }),
  });

  const handleSave = (id: string) => saveRoute(id, getEdit(id));

  const handleCreate = async () => {
    const name = newName.trim() || namePlaceholder || "route";
    try {
      await putRoute.mutateAsync({
        id: encode(crypto.getRandomValues(new Uint8Array(16))),
        name,
        priority: newPriority,
        expression: newExpression,
        destinations: newDestinations.map((d) => d.vaultId),
        distribution: newDistribution,
        enabled: newEnabled,
      });
      addToast(`Route "${name}" created`, "info");
      setAdding(false);
      setNewName("");
      setNewPriority(0);
      setNewExpression("*");
      setNewDestinations([]);
      setNewDistribution("fanout");
      setNewEnabled(true);
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
        setNewPriority(0);
        setNewExpression("*");
        setNewDestinations([]);
        setNewDistribution("fanout");
        setNewEnabled(true);
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
          <FormField
            label="Priority"
            description="Lower priority fires first. Routes with the same priority break ties by name."
            dark={dark}
          >
            <TextInput
              value={String(newPriority)}
              onChange={(v) => setNewPriority(parseInt(v, 10) || 0)}
              dark={dark}
            />
          </FormField>
          <FormField
            label="Match expression"
            description='Match expression evaluated against each record. "*" matches everything. The lean editor with autocomplete and live validation lands later in this epic.'
            dark={dark}
          >
            <TextInput
              value={newExpression}
              onChange={setNewExpression}
              placeholder="*"
              dark={dark}
            />
          </FormField>
          <DestinationsEditor
            destinations={newDestinations}
            onChange={setNewDestinations}
            vaults={vaults.map((v) => ({ id: encode(v.id), name: v.name }))}
            dark={dark}
          />
          <FormField
            label="Distribution"
            description={
              newDestinations.length <= 1
                ? "Only takes effect with two or more destinations."
                : "Fanout sends to all destinations. Round Robin distributes evenly. Failover sends to the first available destination."
            }
            dark={dark}
          >
            <SelectInput
              value={newDistribution}
              onChange={setNewDistribution}
              options={distributionOptions}
              dark={dark}
              disabled={newDestinations.length <= 1}
            />
          </FormField>
        </AddFormCard>
      )}

      {sortByName(routes).map((route) => {
        const id = encode(route.id);
        const edit = getEdit(id);
        const destNames = route.destinations
          .map((d) => vaults.find((v) => encode(v.id) === encode(d.vaultId))?.name || encode(d.vaultId))
          .join(", ");
        const expr = routeExpression(route);
        return (
          <SettingsCard
            key={id}
            id={route.name || id}
            dark={dark}
            expanded={isExpanded(id)}
            onToggle={() => toggleCard(id)}
            onDelete={() => handleDelete(id)}
            typeBadge={route.destinations.length > 1 ? route.distribution || "fanout" : undefined}
            status={
              <span className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                {expr || "no match expression"}
                {destNames ? ` → ${destNames}` : ""}
                {!route.enabled && " (disabled)"}
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
              <FormField
                label="Priority"
                description="Lower priority fires first. Routes with the same priority break ties by name."
                dark={dark}
              >
                <TextInput
                  value={String(edit.priority)}
                  onChange={(v) => setEdit(id, { priority: parseInt(v, 10) || 0 })}
                  dark={dark}
                />
              </FormField>
              <FormField
                label="Match expression"
                description='Match expression evaluated against each record. "*" matches everything. The lean editor with autocomplete and live validation lands later in this epic.'
                dark={dark}
              >
                <TextInput
                  value={edit.expression}
                  onChange={(v) => setEdit(id, { expression: v })}
                  placeholder="*"
                  dark={dark}
                />
              </FormField>
              <DestinationsEditor
                destinations={edit.destinations}
                onChange={(dests) => setEdit(id, { destinations: dests })}
                vaults={vaults.map((v) => ({ id: encode(v.id), name: v.name }))}
                dark={dark}
              />
              <FormField
                label="Distribution"
                description={
                  edit.destinations.length <= 1
                    ? "Only takes effect with two or more destinations."
                    : "Fanout sends to all destinations. Round Robin distributes evenly. Failover sends to the first available destination."
                }
                dark={dark}
              >
                <SelectInput
                  value={edit.distribution}
                  onChange={(v) => setEdit(id, { distribution: v })}
                  options={distributionOptions}
                  dark={dark}
                  disabled={edit.destinations.length <= 1}
                />
              </FormField>
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}

// DestinationsEditor renders a flat checkbox list — every destination is
// explicit. The wire shape is a list of {vaultId}; this component keeps
// that contract by reconstructing it from the checked vault IDs.
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
  const sortedVaults = [...vaults].sort((a, b) => a.name.localeCompare(b.name));
  const selected = new Set(destinations.map((d) => d.vaultId));

  const toggle = (id: string) => {
    if (selected.has(id)) {
      onChange(destinations.filter((d) => d.vaultId !== id));
    } else {
      onChange([...destinations, { vaultId: id }]);
    }
  };

  return (
    <FormField label="Destinations" dark={dark}>
      <div className="flex flex-col gap-1.5">
        {sortedVaults.length === 0 && (
          <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
            No vaults available
          </p>
        )}
        {sortedVaults.map((v) => (
          <Checkbox
            key={v.id}
            checked={selected.has(v.id)}
            onChange={() => toggle(v.id)}
            label={v.name || v.id}
            dark={dark}
          />
        ))}
      </div>
    </FormField>
  );
}
