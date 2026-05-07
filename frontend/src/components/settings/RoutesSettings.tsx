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

function toggleSource(sources: RouteSource[], kind: RouteSource): RouteSource[] {
  if (sources.includes(kind)) return sources.filter((s) => s !== kind);
  return [...sources, kind];
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
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}

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
  const ingest = sourcesIncludeIngest(sources);
  const retention = sourcesIncludeRetention(sources);

  return (
    <FormField
      label="Sources"
      description="A route is consulted whenever ANY of its checked sources is active for the record at hand. Optionally narrow each source to specific ingesters or vaults; leaving the picker empty matches any."
      dark={dark}
    >
      <div className="flex flex-col gap-2">
        <Checkbox
          checked={ingest}
          onChange={() => onSourcesChange(toggleSource(sources, RouteSource.INGEST))}
          label="Match live ingest"
          dark={dark}
        />
        {ingest && (
          <div className="ml-6">
            <IdMultiPicker
              label="Limit to ingesters"
              emptyLabel="All ingesters"
              addLabel="Add ingester…"
              selectedIds={sourceIngesterIds}
              onChange={onSourceIngesterIdsChange}
              items={ingesters}
              dark={dark}
            />
          </div>
        )}
        <Checkbox
          checked={retention}
          onChange={() =>
            onSourcesChange(toggleSource(sources, RouteSource.RETENTION_TRIGGER))
          }
          label="Match retention events"
          dark={dark}
        />
        {retention && (
          <div className="ml-6">
            <IdMultiPicker
              label="Limit to source vaults"
              emptyLabel="Any vault"
              addLabel="Add vault…"
              selectedIds={sourceVaultIds}
              onChange={onSourceVaultIdsChange}
              items={vaults}
              dark={dark}
            />
          </div>
        )}
      </div>
    </FormField>
  );
}

// IdMultiPicker is a generic multi-select picker used for the
// retention-vault and ingest-ingester narrower lists. Empty selection
// is meaningful — represents "match any". gastrolog-42f9z (Phase 4).
function IdMultiPicker({
  label,
  emptyLabel,
  addLabel,
  selectedIds,
  onChange,
  items,
  dark,
}: Readonly<{
  label: string;
  emptyLabel: string;
  addLabel: string;
  selectedIds: string[];
  onChange: (ids: string[]) => void;
  items: { id: string; name: string }[];
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const usedIds = new Set(selectedIds);
  const available = items.filter((it) => !usedIds.has(it.id));

  return (
    <FormField label={label} dark={dark}>
      <div className="flex flex-col gap-1.5">
        {selectedIds.length === 0 && (
          <span className={`text-[0.8em] italic ${c("text-text-muted", "text-light-text-muted")}`}>
            {emptyLabel}
          </span>
        )}
        {selectedIds.map((id) => {
          const item = items.find((it) => it.id === id);
          return (
            <div key={id} className="flex items-center gap-2">
              <span
                className={`flex-1 text-[0.85em] px-2.5 py-1.5 border rounded ${c(
                  "bg-ink-surface border-ink-border text-text-bright",
                  "bg-light-surface border-light-border text-light-text-bright",
                )}`}
              >
                {item?.name || id}
              </span>
              <Button
                variant="ghost"
                onClick={() => onChange(selectedIds.filter((sid) => sid !== id))}
                dark={dark}
              >
                Remove
              </Button>
            </div>
          );
        })}
        {available.length > 0 && (
          <SelectInput
            value=""
            onChange={(v) => {
              if (v) onChange([...selectedIds, v]);
            }}
            options={[
              { value: "", label: addLabel },
              ...available
                .map((it) => ({ value: it.id, label: it.name || it.id }))
                .sort((a, b) => a.label.localeCompare(b.label)),
            ]}
            dark={dark}
          />
        )}
      </div>
    </FormField>
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
          <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
            No vaults available
          </p>
        )}
      </div>
    </FormField>
  );
}
