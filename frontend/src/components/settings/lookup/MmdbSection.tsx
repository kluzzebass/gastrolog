import { encode } from "../../../api/glid";
import { useState } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { useLookupCrud } from "./useLookupCrud";
import { FormField, TextInput } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { FileDropZone } from "../FileDropZone";
import { type MMDBLookupDraft, type LookupSectionProps, mmdbDbTypes, mmdbDefaultName, emptyMmdbDraft, mmdbLookupEqual } from "./types";
import type { MMDBLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";

export function serializeMmdbLookups(lookups: MMDBLookupDraft[]) {
  return lookups
    .filter((m) => m.name)
    .map((m) => ({
      name: m.name,
      dbType: m.dbType,
      fileId: m.fileId || undefined,
    }));
}

export function MmdbAddForm({
  dark,
  managedFiles,
  uploadFile,
  addToast,
  onCreated,
  onCancel,
  existingLookups,
}: LookupSectionProps & {
  onCreated: (draft: MMDBLookupDraft) => void;
  onCancel: () => void;
  existingLookups: MMDBLookupDraft[];
}) {
  const putConfig = usePutSettings();
  const [draft, setDraft] = useState<MMDBLookupDraft>(() => emptyMmdbDraft());

  const handleCreate = async () => {
    const final = { ...draft, name: draft.name.trim() || mmdbDefaultName[draft.dbType] || draft.dbType };
    if (!final.name) return;
    const updated = [...existingLookups, final];
    try {
      await putConfig.mutateAsync({ lookup: { mmdbLookups: serializeMmdbLookups(updated) } });
      onCreated(final);
      addToast(`MMDB lookup "${final.name}" created`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create MMDB lookup", "error");
    }
  };

  return (
    <AddFormCard
      dark={dark}
      onCancel={onCancel}
      onCreate={handleCreate}
      isPending={putConfig.isPending}
      createDisabled={false}
      typeBadge="mmdb"
    >
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup geoip" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => setDraft((d) => ({ ...d, name: v }))} placeholder={mmdbDefaultName[draft.dbType] || draft.dbType} dark={dark} mono />
      </FormField>
      <MmdbDbTypeRadio dark={dark} value={draft.dbType} onChange={(dbType) => setDraft((d) => ({ ...d, dbType }))} name="new-mmdb-db-type" />
      <FormField label="MMDB File" description="Upload a custom MMDB file, or leave empty to use the auto-downloaded database." dark={dark}>
        <FileDropZone
          dark={dark}
          inputId="new-mmdb-upload"
          accept=".mmdb"
          label={draft.dbType === "city" ? "GeoIP City .mmdb" : "ASN .mmdb"}
          currentFile={draft.fileId ? managedFiles.find((f) => encode(f.id) === draft.fileId) : undefined}
          pickableFiles={managedFiles.filter((f) => f.name.endsWith(".mmdb"))}
          uploadFile={uploadFile}
          addToast={addToast}
          onFileSelected={(fileId) => setDraft((d) => ({ ...d, fileId }))}
        />
      </FormField>
    </AddFormCard>
  );
}

export function MmdbCards({
  dark,
  managedFiles,
  uploadFile,
  addToast,
  lookups,
  savedLookups,
  onUpdate,
  onDelete,
  onRevert,
}: LookupSectionProps & {
  lookups: MMDBLookupDraft[];
  savedLookups: MMDBLookupEntry[];
  onUpdate: (i: number, patch: Partial<MMDBLookupDraft>) => void;
  onDelete: (i: number) => void;
  onRevert: (i: number) => void;
}) {
  const c = useThemeClass(dark);
  const { isDirty, save, handleDelete, putConfig } = useLookupCrud({
    lookups, savedLookups, serialize: serializeMmdbLookups, equal: mmdbLookupEqual,
    lookupKey: "mmdbLookups", typeLabel: "MMDB", getName: (m) => m.name, onDelete,
  });
  const { isExpanded, toggle } = useExpandedCards();

  return (
    <>
      {lookups.map((m, i) => {
        const resolvedFile = m.fileId ? managedFiles.find((f) => encode(f.id) === m.fileId) : undefined;
        let dbLabel = m.dbType;
        if (m.dbType === "city") dbLabel = "GeoIP City";
        else if (m.dbType === "asn") dbLabel = "ASN";
        return (
          <SettingsCard
            key={`mmdb-${i}`}
            id={m.name || `MMDB Lookup ${i + 1}`}
            typeBadge="mmdb"
            dark={dark}
            expanded={isExpanded(`mmdb-${i}`)}
            onToggle={() => toggle(`mmdb-${i}`)}
            onDelete={() => handleDelete(i)}
            status={
              <span className={`text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}>
                {dbLabel}{(() => {
                  if (resolvedFile) return ` — ${resolvedFile.name}`;
                  return m.fileId ? "" : " (auto-download)";
                })()}
              </span>
            }
            footer={
              <>
                {isDirty(i) && (
                  <Button onClick={() => onRevert(i)} disabled={putConfig.isPending} dark={dark} variant="ghost">
                    Discard
                  </Button>
                )}
                <Button
                  onClick={() => save(i)}
                  disabled={!isDirty(i) || !m.name || putConfig.isPending}
                >
                  {putConfig.isPending ? "Saving..." : "Save"}
                </Button>
              </>
            }
          >
            <div className="flex flex-col gap-3">
              <FormField label="Name" description="Registry name used in queries, e.g. | lookup geoip" dark={dark}>
                <TextInput value={m.name} onChange={(v) => onUpdate(i, { name: v })} placeholder="" dark={dark} mono />
              </FormField>
              <MmdbDbTypeRadio dark={dark} value={m.dbType} onChange={(dbType) => onUpdate(i, { dbType })} name={`mmdb-db-type-${i}`} />
              <FormField label="MMDB File" description="Upload a custom MMDB file, or leave empty to use the auto-downloaded database." dark={dark}>
                <FileDropZone
                  dark={dark}
                  inputId={`mmdb-upload-${i}`}
                  accept=".mmdb"
                  label={m.dbType === "city" ? "GeoIP City .mmdb" : "ASN .mmdb"}
                  currentFile={resolvedFile}
                  pickableFiles={managedFiles.filter((f) => f.name.endsWith(".mmdb"))}
                  uploadFile={uploadFile}
                  addToast={addToast}
                  onFileSelected={(fileId) => onUpdate(i, { fileId })}
                />
              </FormField>
              {!m.fileId && (
                <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
                  No file uploaded — will use auto-downloaded {dbLabel} database if MaxMind auto-download is enabled.
                </p>
              )}
            </div>
          </SettingsCard>
        );
      })}
    </>
  );
}

function MmdbDbTypeRadio({ dark, value, onChange, name }: Readonly<{ dark: boolean; value: string; onChange: (v: string) => void; name: string }>) {
  const c = useThemeClass(dark);
  return (
    <FormField label="Database Type" dark={dark}>
      <div className="flex gap-3">
        {mmdbDbTypes.map((t) => (
          <label key={t.value} className="flex items-center gap-1.5 cursor-pointer">
            <input
              type="radio"
              name={name}
              value={t.value}
              checked={value === t.value}
              onChange={() => onChange(t.value)}
              className="accent-copper"
            />
            <span className={`text-[0.85em] ${c("text-text-bright", "text-light-text-bright")}`}>{t.label}</span>
          </label>
        ))}
      </div>
    </FormField>
  );
}
