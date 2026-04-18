import { useState } from "react";
import { useSettings } from "../../api/hooks/useSettings";
import { useConfig, useGenerateName } from "../../api/hooks/useSystem";
import { useUploadManagedFile } from "../../api/hooks/useUploadManagedFile";
import { useToast } from "../Toast";
import { SettingsSection } from "./SettingsSection";
import { MmdbAddForm, MmdbCards } from "./lookup/MmdbSection";
import { HttpAddForm, HttpCards } from "./lookup/HttpSection";
import { JsonAddForm, JsonCards } from "./lookup/JsonSection";
import { YamlAddForm, YamlCards } from "./lookup/YamlSection";
import { CsvAddForm, CsvCards } from "./lookup/CsvSection";
import { StaticAddForm, StaticCards } from "./lookup/StaticSection";
import {
  lookupTypes,
  httpEntryToDraft,
  mmdbEntryToDraft,
  jsonFileEntryToDraft,
  yamlFileEntryToDraft,
  csvEntryToDraft,
  staticEntryToDraft,
  type MMDBLookupDraft,
  type HTTPLookupDraft,
  type JSONFileLookupDraft,
  type YAMLFileLookupDraft,
  type CSVLookupDraft,
  type StaticLookupDraft,
} from "./lookup/types";

export function LookupsSettings({ dark }: Readonly<{ dark: boolean }>) {
  const { data, isLoading } = useSettings();
  const { data: config } = useConfig();
  const uploadFile = useUploadManagedFile();
  const { addToast } = useToast();
  const generateName = useGenerateName();

  const managedFiles = config?.managedFiles ?? [];

  // -- State -----------------------------------------------------------------
  const [initialized, setInitialized] = useState(false);
  const [mmdbLookups, setMmdbLookups] = useState<MMDBLookupDraft[]>([]);
  const [httpLookups, setHttpLookups] = useState<HTTPLookupDraft[]>([]);
  const [jsonFileLookups, setJsonFileLookups] = useState<JSONFileLookupDraft[]>([]);
  const [yamlFileLookups, setYamlFileLookups] = useState<YAMLFileLookupDraft[]>([]);
  const [csvLookups, setCsvLookups] = useState<CSVLookupDraft[]>([]);
  const [staticLookups, setStaticLookups] = useState<StaticLookupDraft[]>([]);
  const [addingType, setAddingType] = useState<string | null>(null);
  const [namePlaceholder, setNamePlaceholder] = useState("");

  // -- Init from server (once) -----------------------------------------------
  if (data && !initialized) {
    setMmdbLookups((data.lookup?.mmdbLookups ?? []).map(mmdbEntryToDraft));
    setHttpLookups((data.lookup?.httpLookups ?? []).map(httpEntryToDraft));
    setJsonFileLookups((data.lookup?.jsonFileLookups ?? []).map(jsonFileEntryToDraft));
    setYamlFileLookups((data.lookup?.yamlFileLookups ?? []).map(yamlFileEntryToDraft));
    setCsvLookups((data.lookup?.csvLookups ?? []).map(csvEntryToDraft));
    setStaticLookups((data.lookup?.staticLookups ?? []).map(staticEntryToDraft));
    setInitialized(true);
  }

  // -- Add form handler ------------------------------------------------------
  const handleAddSelect = (type: string) => {
    setAddingType(type);
    if (type !== "mmdb") {
      generateName.mutateAsync().then(setNamePlaceholder).catch(() => {});
    }
  };

  const closeAdd = () => setAddingType(null);
  const isEmpty = mmdbLookups.length === 0 && httpLookups.length === 0 && jsonFileLookups.length === 0 && yamlFileLookups.length === 0 && csvLookups.length === 0 && staticLookups.length === 0;
  const sectionProps = { dark, managedFiles, uploadFile, addToast };

  // -- Render -----------------------------------------------------------------
  return (
    <SettingsSection
      addLabel="Add Lookup"
      adding={!!addingType}
      onToggleAdd={() => setAddingType(null)}
      addOptions={lookupTypes}
      onAddSelect={handleAddSelect}
      isLoading={isLoading}
      isEmpty={isEmpty}
      emptyMessage='No lookups configured. Click "Add Lookup" to create one.'
      dark={dark}
    >
      {/* Add forms */}
      {addingType === "mmdb" && (
        <MmdbAddForm
          {...sectionProps}
          existingLookups={mmdbLookups}
          onCreated={(draft) => { setMmdbLookups((prev) => [...prev, draft]); closeAdd(); }}
          onCancel={closeAdd}
        />
      )}
      {addingType === "http" && (
        <HttpAddForm
          dark={dark}
          addToast={addToast}
          existingLookups={httpLookups}
          namePlaceholder={namePlaceholder}
          onCreated={(draft) => { setHttpLookups((prev) => [...prev, draft]); closeAdd(); }}
          onCancel={closeAdd}
        />
      )}
      {addingType === "json" && (
        <JsonAddForm
          {...sectionProps}
          existingLookups={jsonFileLookups}
          namePlaceholder={namePlaceholder}
          onCreated={(draft) => { setJsonFileLookups((prev) => [...prev, draft]); closeAdd(); }}
          onCancel={closeAdd}
        />
      )}
      {addingType === "yaml" && (
        <YamlAddForm
          {...sectionProps}
          existingLookups={yamlFileLookups}
          namePlaceholder={namePlaceholder}
          onCreated={(draft) => { setYamlFileLookups((prev) => [...prev, draft]); closeAdd(); }}
          onCancel={closeAdd}
        />
      )}
      {addingType === "csv" && (
        <CsvAddForm
          {...sectionProps}
          existingLookups={csvLookups}
          namePlaceholder={namePlaceholder}
          onCreated={(draft) => { setCsvLookups((prev) => [...prev, draft]); closeAdd(); }}
          onCancel={closeAdd}
        />
      )}
      {addingType === "static" && (
        <StaticAddForm
          dark={dark}
          addToast={addToast}
          existingLookups={staticLookups}
          namePlaceholder={namePlaceholder}
          onCreated={(draft) => { setStaticLookups((prev) => [...prev, draft]); closeAdd(); }}
          onCancel={closeAdd}
        />
      )}

      {/* Entity cards */}
      <MmdbCards
        {...sectionProps}
        lookups={mmdbLookups}
        savedLookups={data?.lookup?.mmdbLookups ?? []}
        onUpdate={(i, patch) => setMmdbLookups((prev) => prev.map((m, j) => (j === i ? { ...m, ...patch } : m)))}
        onDelete={(i) => setMmdbLookups((prev) => prev.filter((_, j) => j !== i))}
        onRevert={(i) => {
          const saved = data?.lookup?.mmdbLookups[i];
          if (saved) setMmdbLookups((prev) => prev.map((m, j) => j === i ? mmdbEntryToDraft(saved) : m));
        }}
      />
      <HttpCards
        dark={dark}
        addToast={addToast}
        lookups={httpLookups}
        savedLookups={data?.lookup?.httpLookups ?? []}
        onUpdate={(i, patch) => setHttpLookups((prev) => prev.map((h, j) => (j === i ? { ...h, ...patch } : h)))}
        onDelete={(i) => setHttpLookups((prev) => prev.filter((_, j) => j !== i))}
        onRevert={(i) => {
          const saved = data?.lookup?.httpLookups[i];
          if (saved) setHttpLookups((prev) => prev.map((h, j) => j === i ? httpEntryToDraft(saved) : h));
        }}
      />
      <JsonCards
        {...sectionProps}
        lookups={jsonFileLookups}
        savedLookups={data?.lookup?.jsonFileLookups ?? []}
        onUpdate={(i, patch) => setJsonFileLookups((prev) => prev.map((j, k) => (k === i ? { ...j, ...patch } : j)))}
        onDelete={(i) => setJsonFileLookups((prev) => prev.filter((_, j) => j !== i))}
        onRevert={(i) => {
          const saved = data?.lookup?.jsonFileLookups[i];
          if (saved) setJsonFileLookups((prev) => prev.map((j, k) => k === i ? jsonFileEntryToDraft(saved) : j));
        }}
      />
      <YamlCards
        {...sectionProps}
        lookups={yamlFileLookups}
        savedLookups={data?.lookup?.yamlFileLookups ?? []}
        onUpdate={(i, patch) => setYamlFileLookups((prev) => prev.map((y, k) => (k === i ? { ...y, ...patch } : y)))}
        onDelete={(i) => setYamlFileLookups((prev) => prev.filter((_, j) => j !== i))}
        onRevert={(i) => {
          const saved = data?.lookup?.yamlFileLookups[i];
          if (saved) setYamlFileLookups((prev) => prev.map((y, k) => k === i ? yamlFileEntryToDraft(saved) : y));
        }}
      />
      <CsvCards
        {...sectionProps}
        lookups={csvLookups}
        savedLookups={data?.lookup?.csvLookups ?? []}
        onUpdate={(i, patch) => setCsvLookups((prev) => prev.map((c, j) => (j === i ? { ...c, ...patch } : c)))}
        onDelete={(i) => setCsvLookups((prev) => prev.filter((_, j) => j !== i))}
        onRevert={(i) => {
          const saved = data?.lookup?.csvLookups[i];
          if (saved) setCsvLookups((prev) => prev.map((c, j) => j === i ? csvEntryToDraft(saved) : c));
        }}
      />
      <StaticCards
        dark={dark}
        lookups={staticLookups}
        savedLookups={data?.lookup?.staticLookups ?? []}
        onUpdate={(i, patch) => setStaticLookups((prev) => prev.map((s, j) => (j === i ? { ...s, ...patch } : s)))}
        onDelete={(i) => setStaticLookups((prev) => prev.filter((_, j) => j !== i))}
        onRevert={(i) => {
          const saved = data?.lookup?.staticLookups[i];
          if (saved) setStaticLookups((prev) => prev.map((s, j) => j === i ? staticEntryToDraft(saved) : s));
        }}
      />
    </SettingsSection>
  );
}
