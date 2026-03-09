import type { HTTPLookupEntry, JSONFileLookupEntry, ManagedFileInfo, MMDBLookupEntry } from "../../../api/gen/gastrolog/v1/config_pb";
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
  responsePaths: string[];
  parameters: LookupParamDraft[];
}

export interface MMDBLookupDraft {
  name: string;
  dbType: string; // "city" or "asn"
  fileId: string; // managed file ID; empty = auto-download
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const lookupTypes = [
  { value: "mmdb", label: "MMDB (GeoIP / ASN)" },
  { value: "http", label: "HTTP" },
  { value: "json", label: "JSON File" },
  { value: "maxmind", label: "MaxMind Auto-Download" },
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
  return { name: "", fileId: "", query: "", responsePaths: [], parameters: [] };
}

export function emptyMmdbDraft(): MMDBLookupDraft {
  return { name: "", dbType: "city", fileId: "" };
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
  return draft.name === saved.name && draft.dbType === saved.dbType && draft.fileId === saved.fileId;
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
  if (!paramsEqual(draft.parameters, saved.parameters ?? [])) return false;
  const dKeys = Object.keys(draft.headers);
  const sKeys = Object.keys(saved.headers);
  if (dKeys.length !== sKeys.length) return false;
  for (const k of dKeys) {
    if (draft.headers[k] !== saved.headers[k]) return false;
  }
  return true;
}

export function jsonFileLookupEqual(draft: JSONFileLookupDraft, saved: JSONFileLookupEntry): boolean {
  if (
    draft.name !== saved.name ||
    draft.fileId !== saved.fileId ||
    draft.query !== saved.query ||
    !arraysEqual(draft.responsePaths, saved.responsePaths)
  ) return false;
  return paramsEqual(draft.parameters, saved.parameters ?? []);
}
