import { encode } from "../../../api/glid";
import type { CSVLookupEntry, HTTPLookupEntry, JSONFileLookupEntry, ManagedFileInfo, MMDBLookupEntry, StaticLookupEntry, YAMLFileLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";
import type { useUploadManagedFile } from "../../../api/hooks/useUploadManagedFile";

// ---------------------------------------------------------------------------
// Draft types (local form state before save)
// ---------------------------------------------------------------------------

export interface LookupParamDraft {
  name: string;
  description: string;
}

export interface HTTPLookupDraft {
  name: string;
  urlTemplate: string;
  headers: Record<string, string>;
  responsePaths: string[];
  parameters: LookupParamDraft[];
  timeout: string;
  cacheTtl: string;
  cacheSize: number;
}

export interface JSONFileLookupDraft {
  name: string;
  fileId: string;
  query: string;
  keyColumn: string;
  valueColumns: string[];
}

export interface MMDBLookupDraft {
  name: string;
  dbType: string; // "city" or "asn"
  fileId: string; // managed file ID; empty = auto-download
}

export interface CSVLookupDraft {
  name: string;
  fileId: string;
  keyColumn: string;
  valueColumns: string[];
}

export interface StaticLookupDraft {
  name: string;
  keyColumn: string;
  valueColumns: string[];
  rows: Record<string, string>[];
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const lookupTypes = [
  { value: "mmdb", label: "MMDB (GeoIP / ASN)" },
  { value: "http", label: "HTTP" },
  { value: "json", label: "JSON File" },
  { value: "yaml", label: "YAML File" },
  { value: "csv", label: "CSV File" },
  { value: "static", label: "Static" },
];

export const mmdbDbTypes = [
  { value: "city", label: "GeoIP City" },
  { value: "asn", label: "ASN" },
];

export const mmdbDefaultName: Record<string, string> = { city: "geoip", asn: "asn" };

// ---------------------------------------------------------------------------
// Empty draft factories
// ---------------------------------------------------------------------------

export function emptyHttpDraft(): HTTPLookupDraft {
  return { name: "", urlTemplate: "", headers: {}, responsePaths: [], parameters: [], timeout: "", cacheTtl: "", cacheSize: 0 };
}

export function emptyJsonDraft(): JSONFileLookupDraft {
  return { name: "", fileId: "", query: "", keyColumn: "", valueColumns: [] };
}

export function emptyMmdbDraft(): MMDBLookupDraft {
  return { name: "", dbType: "city", fileId: "" };
}

export function emptyCsvDraft(): CSVLookupDraft {
  return { name: "", fileId: "", keyColumn: "", valueColumns: [] };
}

export function emptyStaticDraft(): StaticLookupDraft {
  return { name: "", keyColumn: "", valueColumns: [], rows: [] };
}

// ---------------------------------------------------------------------------
// Saved-entry → draft converters (used by init + Discard revert)
// ---------------------------------------------------------------------------

export function httpEntryToDraft(h: HTTPLookupEntry): HTTPLookupDraft {
  return {
    name: h.name,
    urlTemplate: h.urlTemplate,
    headers: { ...h.headers },
    responsePaths: [...h.responsePaths],
    parameters: h.parameters.map((p) => ({ name: p.name, description: p.description })),
    timeout: h.timeout,
    cacheTtl: h.cacheTtl,
    cacheSize: h.cacheSize,
  };
}

export function mmdbEntryToDraft(m: MMDBLookupEntry): MMDBLookupDraft {
  return { name: m.name, dbType: m.dbType, fileId: encode(m.fileId) };
}

export function jsonFileEntryToDraft(j: JSONFileLookupEntry): JSONFileLookupDraft {
  return {
    name: j.name,
    fileId: encode(j.fileId),
    query: j.query,
    keyColumn: j.keyColumn,
    valueColumns: [...j.valueColumns],
  };
}

export function yamlFileEntryToDraft(y: YAMLFileLookupEntry): YAMLFileLookupDraft {
  return {
    name: y.name,
    fileId: encode(y.fileId),
    query: y.query,
    keyColumn: y.keyColumn,
    valueColumns: [...y.valueColumns],
  };
}

export function csvEntryToDraft(c: CSVLookupEntry): CSVLookupDraft {
  return {
    name: c.name,
    fileId: encode(c.fileId),
    keyColumn: c.keyColumn,
    valueColumns: [...c.valueColumns],
  };
}

export function staticEntryToDraft(s: StaticLookupEntry): StaticLookupDraft {
  return {
    name: s.name,
    keyColumn: s.keyColumn,
    valueColumns: [...s.valueColumns],
    rows: s.rows.map((r) => ({ ...r.values })),
  };
}

// ---------------------------------------------------------------------------
// Shared props passed from orchestrator to each section
// ---------------------------------------------------------------------------

export interface LookupSectionProps {
  dark: boolean;
  managedFiles: ManagedFileInfo[];
  uploadFile: ReturnType<typeof useUploadManagedFile>;
  addToast: (msg: string, type: "info" | "error") => void;
}

// ---------------------------------------------------------------------------
// Equality utilities (dirty checking)
// ---------------------------------------------------------------------------

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function paramsEqual(a: LookupParamDraft[], b: { name: string; description: string }[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i]!.name !== b[i]!.name || a[i]!.description !== b[i]!.description) return false;
  }
  return true;
}

export function mmdbLookupEqual(draft: MMDBLookupDraft, saved: MMDBLookupEntry): boolean {
  return draft.name === saved.name && draft.dbType === saved.dbType && draft.fileId === encode(saved.fileId);
}

export function httpLookupEqual(draft: HTTPLookupDraft, saved: HTTPLookupEntry): boolean {
  if (
    draft.name !== saved.name ||
    draft.urlTemplate !== saved.urlTemplate ||
    !arraysEqual(draft.responsePaths, saved.responsePaths) ||
    draft.timeout !== saved.timeout ||
    draft.cacheTtl !== saved.cacheTtl ||
    draft.cacheSize !== saved.cacheSize
  ) return false;
  if (!paramsEqual(draft.parameters, saved.parameters)) return false;
  const dKeys = Object.keys(draft.headers);
  const sKeys = Object.keys(saved.headers);
  if (dKeys.length !== sKeys.length) return false;
  for (const k of dKeys) {
    if (draft.headers[k] !== saved.headers[k]) return false;
  }
  return true;
}

export function csvLookupEqual(draft: CSVLookupDraft, saved: CSVLookupEntry): boolean {
  return (
    draft.name === saved.name &&
    draft.fileId === encode(saved.fileId) &&
    draft.keyColumn === saved.keyColumn &&
    arraysEqual(draft.valueColumns, saved.valueColumns)
  );
}

export function jsonFileLookupEqual(draft: JSONFileLookupDraft, saved: JSONFileLookupEntry): boolean {
  return (
    draft.name === saved.name &&
    draft.fileId === encode(saved.fileId) &&
    draft.query === saved.query &&
    draft.keyColumn === saved.keyColumn &&
    arraysEqual(draft.valueColumns, saved.valueColumns)
  );
}

// YAMLFileLookupDraft shares the JSON draft shape — the jq/key/value
// semantics are identical; only the on-disk file format differs.
export type YAMLFileLookupDraft = JSONFileLookupDraft;

export function emptyYamlDraft(): YAMLFileLookupDraft {
  return emptyJsonDraft();
}

export function yamlFileLookupEqual(draft: YAMLFileLookupDraft, saved: YAMLFileLookupEntry): boolean {
  return jsonFileLookupEqual(draft, saved);
}

export function staticLookupEqual(draft: StaticLookupDraft, saved: StaticLookupEntry): boolean {
  if (
    draft.name !== saved.name ||
    draft.keyColumn !== saved.keyColumn ||
    !arraysEqual(draft.valueColumns, saved.valueColumns)
  ) return false;
  if (draft.rows.length !== saved.rows.length) return false;
  for (let i = 0; i < draft.rows.length; i++) {
    const dRow = draft.rows[i]!;
    const sRow = saved.rows[i]!.values;
    const dKeys = Object.keys(dRow);
    const sKeys = Object.keys(sRow);
    if (dKeys.length !== sKeys.length) return false;
    for (const k of dKeys) {
      if (dRow[k] !== sRow[k]) return false;
    }
  }
  return true;
}
