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
import { RouteSource } from "../../api/gen/gastrolog/v1/system_pb";

// Phase 4 (gastrolog-42f9z): RouteSource is a multi-select set of
// source-predicate kinds. Empty = INGEST default for back-compat.
function sourcesIncludeIngest(sources: RouteSource[]): boolean {
  if (sources.length === 0) return true; // empty defaults to INGEST
  return sources.some(
    (s) => s === RouteSource.INGEST || s === RouteSource.UNSPECIFIED,
  );
}

function sourcesIncludeRetention(sources: RouteSource[]): boolean {
  return sources.includes(RouteSource.RETENTION_TRIGGER);
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
  const [newFilterId, setNewFilterId] = useState("");
  const [newDestinations, setNewDestinations] = useState<DestinationEdit[]>([]);
  const [newDistribution, setNewDistribution] = useState("fanout");
  const [newEnabled, setNewEnabled] = useState(true);
  const [newSources, setNewSources] = useState<RouteSource[]>([RouteSource.INGEST]);
  const [newSourceVaultIds, setNewSourceVaultIds] = useState<string[]>([]);
  const [newSourceIngesterIds, setNewSourceIngesterIds] = useState<string[]>([]);

  const routes = config?.routes ?? [];
  const filters = config?.filters ?? [];
  const vaults = config?.vaults ?? [];
  const ingesters = config?.ingesters ?? [];
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
        sources: [RouteSource.INGEST] as RouteSource[],
        sourceVaultIds: [] as string[],
        sourceIngesterIds: [] as string[],
      };
    return {
      name: route.name,
      filterId: encode(route.filterId),
      destinations: route.destinations.map((d) => ({ vaultId: encode(d.vaultId) })),
      distribution: route.distribution || "fanout",
      enabled: route.enabled,
      sources:
        route.sources.length > 0 ? [...route.sources] : [RouteSource.INGEST],
      sourceVaultIds: route.sourceVaultIds.map(encode),
      sourceIngesterIds: route.sourceIngesterIds.map(encode),
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
        sources: RouteSource[];
        sourceVaultIds: string[];
        sourceIngesterIds: string[];
      },
    ) => ({
      id,
      name: edit.name,
      filterId: edit.filterId,
      destinations: edit.destinations.map((d) => d.vaultId),
      distribution: edit.distribution,
      enabled: edit.enabled,
      sources: edit.sources,
      sourceVaultIds: sourcesIncludeRetention(edit.sources) ? edit.sourceVaultIds : [],
      sourceIngesterIds: sourcesIncludeIngest(edit.sources) ? edit.sourceIngesterIds : [],
    }),
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
        sources: newSources,
        sourceVaultIds: sourcesIncludeRetention(newSources) ? newSourceVaultIds : [],
        sourceIngesterIds: sourcesIncludeIngest(newSources) ? newSourceIngesterIds : [],
      });
      addToast(`Route "${name}" created`, "info");
      setAdding(false);
      setNewName("");
      setNewFilterId("");
      setNewDestinations([]);
      setNewDistribution("fanout");
      setNewEnabled(true);
      setNewSources([RouteSource.INGEST]);
      setNewSourceVaultIds([]);
      setNewSourceIngesterIds([]);
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
        setNewSources([RouteSource.INGEST]);
        setNewSourceVaultIds([]);
        setNewSourceIngesterIds([]);
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
          <SourceEditor
            sources={newSources}
            onSourcesChange={setNewSources}
            sourceVaultIds={newSourceVaultIds}
            onSourceVaultIdsChange={setNewSourceVaultIds}
            sourceIngesterIds={newSourceIngesterIds}
            onSourceIngesterIdsChange={setNewSourceIngesterIds}
            vaults={vaults.map((v) => ({ id: encode(v.id), name: v.name }))}
            ingesters={ingesters.map((i) => ({ id: encode(i.id), name: i.name }))}
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
            typeBadge={route.destinations.length > 1 ? route.distribution || "fanout" : undefined}
            status={
              <span className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                {filterName || "no filter"}
                {destNames ? ` \u2192 ${destNames}` : ""}
                {!route.enabled && " (disabled)"}
                {sourcesIncludeIngest([...route.sources]) && " (ingest)"}
                {sourcesIncludeRetention([...route.sources]) && " (retention)"}
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
              <SourceEditor
                sources={edit.sources}
                onSourcesChange={(s) => setEdit(id, { sources: s })}
                sourceVaultIds={edit.sourceVaultIds}
                onSourceVaultIdsChange={(ids) => setEdit(id, { sourceVaultIds: ids })}
                sourceIngesterIds={edit.sourceIngesterIds}
                onSourceIngesterIdsChange={(ids) => setEdit(id, { sourceIngesterIds: ids })}
                vaults={vaults.map((v) => ({ id: encode(v.id), name: v.name }))}
                ingesters={ingesters.map((i) => ({ id: encode(i.id), name: i.name }))}
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

// SourceEditor renders one flat list of checkboxes covering every
// possible source predicate the route can match: a per-section "Any"
// row, then one row per concrete ingester / vault. The wire shape
// (sources set + sourceVaultIds + sourceIngesterIds, with empty
// narrower = "any") is reconstructed from the checked rows on save.
//
// "Any" within a kind is mutually exclusive with that kind's specific
// rows — checking a specific ingester clears "Any ingester", and
// vice versa. Across kinds the boxes are independent: a route can
// match "Any ingester" AND "retention from vault-alpha" at once.
//
// Phase 4 (gastrolog-42f9z).
function SourceEditor({
  sources,
  onSourcesChange,
  sourceVaultIds,
  onSourceVaultIdsChange,
  sourceIngesterIds,
  onSourceIngesterIdsChange,
  vaults,
  ingesters,
  dark,
}: Readonly<{
  sources: RouteSource[];
  onSourcesChange: (s: RouteSource[]) => void;
  sourceVaultIds: string[];
  onSourceVaultIdsChange: (ids: string[]) => void;
  sourceIngesterIds: string[];
  onSourceIngesterIdsChange: (ids: string[]) => void;
  vaults: { id: string; name: string }[];
  ingesters: { id: string; name: string }[];
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const ingestChecked = sourcesIncludeIngest(sources);
  const retentionChecked = sourcesIncludeRetention(sources);

  // "Any X" is checked when the kind is in `sources` AND the narrower
  // list for that kind is empty.
  const anyIngester = ingestChecked && sourceIngesterIds.length === 0;
  const anyVault = retentionChecked && sourceVaultIds.length === 0;

  // Toggling "Any ingester" / "Any vault":
  //  - check: ensure the kind is in `sources` and clear the narrower list.
  //  - uncheck (with no specific rows): drop the kind from `sources`.
  const enableAnyIngester = () => {
    if (!ingestChecked) onSourcesChange([...sources, RouteSource.INGEST]);
    if (sourceIngesterIds.length > 0) onSourceIngesterIdsChange([]);
  };
  const disableAnyIngester = () => {
    if (sourceIngesterIds.length === 0) {
      onSourcesChange(sources.filter((s) => s !== RouteSource.INGEST));
    }
  };
  const enableAnyVault = () => {
    if (!retentionChecked) onSourcesChange([...sources, RouteSource.RETENTION_TRIGGER]);
    if (sourceVaultIds.length > 0) onSourceVaultIdsChange([]);
  };
  const disableAnyVault = () => {
    if (sourceVaultIds.length === 0) {
      onSourcesChange(sources.filter((s) => s !== RouteSource.RETENTION_TRIGGER));
    }
  };

  // Toggle a specific ingester / vault row. Checking a specific row
  // implicitly enables the kind and clears the "Any" implication.
  // Unchecking the last specific row (with "Any" off) drops the kind.
  const toggleIngester = (id: string) => {
    const has = sourceIngesterIds.includes(id);
    const nextIds = has
      ? sourceIngesterIds.filter((x) => x !== id)
      : [...sourceIngesterIds, id];
    onSourceIngesterIdsChange(nextIds);
    if (!has && !ingestChecked) {
      onSourcesChange([...sources, RouteSource.INGEST]);
    } else if (has && nextIds.length === 0 && ingestChecked && !anyIngester) {
      // Was the last specific ingester and "Any" wasn't holding the kind on.
      onSourcesChange(sources.filter((s) => s !== RouteSource.INGEST));
    }
  };
  const toggleVault = (id: string) => {
    const has = sourceVaultIds.includes(id);
    const nextIds = has
      ? sourceVaultIds.filter((x) => x !== id)
      : [...sourceVaultIds, id];
    onSourceVaultIdsChange(nextIds);
    if (!has && !retentionChecked) {
      onSourcesChange([...sources, RouteSource.RETENTION_TRIGGER]);
    } else if (has && nextIds.length === 0 && retentionChecked && !anyVault) {
      onSourcesChange(sources.filter((s) => s !== RouteSource.RETENTION_TRIGGER));
    }
  };

  const sortedIngesters = [...ingesters].sort((a, b) => a.name.localeCompare(b.name));
  const sortedVaults = [...vaults].sort((a, b) => a.name.localeCompare(b.name));

  const sectionHeader = (text: string) => (
    <div
      className={`text-[0.7em] uppercase tracking-wide font-medium pt-1 ${c(
        "text-text-muted",
        "text-light-text-muted",
      )}`}
    >
      {text}
    </div>
  );

  return (
    <FormField
      label="Sources"
      description="Tick every source this route should match. Across kinds (live ingest / retention) the boxes are independent. Within a kind, 'Any' is mutually exclusive with the specific rows."
      dark={dark}
    >
      <div className="flex flex-col gap-1.5">
        {sectionHeader("Live ingest")}
        <Checkbox
          checked={anyIngester}
          onChange={(checked) => (checked ? enableAnyIngester() : disableAnyIngester())}
          label="Any ingester"
          dark={dark}
        />
        {sortedIngesters.map((ing) => (
          <Checkbox
            key={ing.id}
            checked={sourceIngesterIds.includes(ing.id)}
            onChange={() => toggleIngester(ing.id)}
            label={`Ingester: ${ing.name || ing.id}`}
            dark={dark}
          />
        ))}
        {sectionHeader("Retention events")}
        <Checkbox
          checked={anyVault}
          onChange={(checked) => (checked ? enableAnyVault() : disableAnyVault())}
          label="Any vault"
          dark={dark}
        />
        {sortedVaults.map((v) => (
          <Checkbox
            key={v.id}
            checked={sourceVaultIds.includes(v.id)}
            onChange={() => toggleVault(v.id)}
            label={`Retention from: ${v.name || v.id}`}
            dark={dark}
          />
        ))}
      </div>
    </FormField>
  );
}

// DestinationsEditor renders a flat checkbox list \u2014 same mechanism as
// SourceEditor's per-kind list, just without the "Any" row (every
// destination is explicit). The wire shape is a list of {vaultId};
// this component keeps that contract by reconstructing it from the
// checked vault IDs.
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
