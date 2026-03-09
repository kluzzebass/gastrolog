import { useState } from "react";
import { useSettings } from "../../api/hooks/useSettings";
import { useConfig, useGenerateName } from "../../api/hooks/useConfig";
import { useUploadManagedFile } from "../../api/hooks/useUploadManagedFile";
import { useExpandedCard } from "../../hooks/useExpandedCards";
import { useToast } from "../Toast";
import { SettingsSection } from "./SettingsSection";
import { MaxMindCard } from "./lookup/MaxMindCard";
import { MmdbAddForm, MmdbCards } from "./lookup/MmdbSection";
import { HttpAddForm, HttpCards } from "./lookup/HttpSection";
import { JsonAddForm, JsonCards } from "./lookup/JsonSection";
import { lookupTypes, type MMDBLookupDraft, type HTTPLookupDraft, type JSONFileLookupDraft } from "./lookup/types";

export function LookupsSettings({ dark }: Readonly<{ dark: boolean }>) {
  const { data, isLoading } = useSettings();
  const { data: config } = useConfig();
  const uploadFile = useUploadManagedFile();
  const { addToast } = useToast();
  const generateName = useGenerateName();
  const { toggle } = useExpandedCard();

  const managedFiles = config?.managedFiles ?? [];

  // -- State -----------------------------------------------------------------
  const [initialized, setInitialized] = useState(false);
  const [maxmindVisible, setMaxmindVisible] = useState(false);
  const [mmdbLookups, setMmdbLookups] = useState<MMDBLookupDraft[]>([]);
  const [httpLookups, setHttpLookups] = useState<HTTPLookupDraft[]>([]);
  const [jsonFileLookups, setJsonFileLookups] = useState<JSONFileLookupDraft[]>([]);
  const [addingType, setAddingType] = useState<string | null>(null);
  const [namePlaceholder, setNamePlaceholder] = useState("");

  // -- Init from server (once) -----------------------------------------------
  if (data && !initialized) {
    const mm = data.lookup?.maxmind;
    setMaxmindVisible((mm?.autoDownload ?? false) || (mm?.licenseConfigured ?? false));
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
        responsePaths: [...(h.responsePaths ?? [])],
        parameters: (h.parameters ?? []).map((p) => ({ name: p.name, description: p.description })),
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
        responsePaths: [...(j.responsePaths ?? [])],
        parameters: (j.parameters ?? []).map((p) => ({ name: p.name, description: p.description })),
      })),
    );
    setInitialized(true);
  }

  // -- Add form handler ------------------------------------------------------
  const handleAddSelect = (type: string) => {
    if (type === "maxmind") {
      if (!maxmindVisible) setMaxmindVisible(true);
      toggle("maxmind");
      return;
    }
    setAddingType(type);
    if (type !== "mmdb") {
      generateName.mutateAsync().then(setNamePlaceholder);
    }
  };

  const closeAdd = () => setAddingType(null);
  const isEmpty = !maxmindVisible && mmdbLookups.length === 0 && httpLookups.length === 0 && jsonFileLookups.length === 0;
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

      {/* Entity cards */}
      <MaxMindCard
        dark={dark}
        visible={maxmindVisible}
        setVisible={setMaxmindVisible}
        savedMaxmind={data?.lookup?.maxmind}
        addToast={addToast}
      />
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
    </SettingsSection>
  );
}
