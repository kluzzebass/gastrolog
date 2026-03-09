import { useState } from "react";
import { useSettings } from "../../api/hooks/useSettings";
import { useConfig, useGenerateName } from "../../api/hooks/useConfig";
import { useUploadManagedFile } from "../../api/hooks/useUploadManagedFile";
import { useToast } from "../Toast";
import { SettingsSection } from "./SettingsSection";
import { MmdbAddForm, MmdbCards } from "./lookup/MmdbSection";
import { HttpAddForm, HttpCards } from "./lookup/HttpSection";
import { JsonAddForm, JsonCards } from "./lookup/JsonSection";
import { CsvAddForm, CsvCards } from "./lookup/CsvSection";
import { lookupTypes, type MMDBLookupDraft, type HTTPLookupDraft, type JSONFileLookupDraft, type CSVLookupDraft } from "./lookup/types";

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
  const [csvLookups, setCsvLookups] = useState<CSVLookupDraft[]>([]);
  const [addingType, setAddingType] = useState<string | null>(null);
  const [namePlaceholder, setNamePlaceholder] = useState("");

  // -- Init from server (once) -----------------------------------------------
  if (data && !initialized) {
    setMmdbLookups(
      (data.lookup?.mmdbLookups ?? []).map((m) => ({
        name: m.name,
        dbType: m.dbType,
        fileId: m.fileId,
      })),
    );
    setHttpLookups(
      (data.lookup?.httpLookups ?? []).map((h) => ({
        name: h.name,
        urlTemplate: h.urlTemplate,
        headers: { ...h.headers },
        responsePaths: [...h.responsePaths],
        parameters: h.parameters.map((p) => ({ name: p.name, description: p.description })),
        timeout: h.timeout,
        cacheTtl: h.cacheTtl,
        cacheSize: h.cacheSize,
      })),
    );
    setJsonFileLookups(
      (data.lookup?.jsonFileLookups ?? []).map((j) => ({
        name: j.name,
        fileId: j.fileId,
        query: j.query,
        responsePaths: [...j.responsePaths],
        parameters: j.parameters.map((p) => ({ name: p.name, description: p.description })),
      })),
    );
    setCsvLookups(
      (data.lookup?.csvLookups ?? []).map((c) => ({
        name: c.name,
        fileId: c.fileId,
        keyColumn: c.keyColumn,
        valueColumns: [...c.valueColumns],
      })),
    );
    setInitialized(true);
  }

  // -- Add form handler ------------------------------------------------------
  const handleAddSelect = (type: string) => {
    setAddingType(type);
    if (type !== "mmdb") {
      generateName.mutateAsync().then(setNamePlaceholder);
    }
  };

  const closeAdd = () => setAddingType(null);
  const isEmpty = mmdbLookups.length === 0 && httpLookups.length === 0 && jsonFileLookups.length === 0 && csvLookups.length === 0;
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
      {addingType === "csv" && (
        <CsvAddForm
          {...sectionProps}
          existingLookups={csvLookups}
          namePlaceholder={namePlaceholder}
          onCreated={(draft) => { setCsvLookups((prev) => [...prev, draft]); closeAdd(); }}
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
      />
      <HttpCards
        dark={dark}
        addToast={addToast}
        lookups={httpLookups}
        savedLookups={data?.lookup?.httpLookups ?? []}
        onUpdate={(i, patch) => setHttpLookups((prev) => prev.map((h, j) => (j === i ? { ...h, ...patch } : h)))}
        onDelete={(i) => setHttpLookups((prev) => prev.filter((_, j) => j !== i))}
      />
      <JsonCards
        {...sectionProps}
        lookups={jsonFileLookups}
        savedLookups={data?.lookup?.jsonFileLookups ?? []}
        onUpdate={(i, patch) => setJsonFileLookups((prev) => prev.map((j, k) => (k === i ? { ...j, ...patch } : j)))}
        onDelete={(i) => setJsonFileLookups((prev) => prev.filter((_, j) => j !== i))}
      />
      <CsvCards
        {...sectionProps}
        lookups={csvLookups}
        savedLookups={data?.lookup?.csvLookups ?? []}
        onUpdate={(i, patch) => setCsvLookups((prev) => prev.map((c, j) => (j === i ? { ...c, ...patch } : c)))}
        onDelete={(i) => setCsvLookups((prev) => prev.filter((_, j) => j !== i))}
      />
    </SettingsSection>
  );
}
