import { usePreviewJSONLookup, usePreviewYAMLLookup } from "../../../api/hooks/useSettings";
import type { JSONFileLookupEntry, YAMLFileLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";
import { jsonFileLookupEqual, yamlFileLookupEqual, type JSONFileLookupDraft } from "./types";

// FileLookupFormat tags which structured format a section instance serves.
// Drives preview hook, file-picker extension, type badge, and payload key.
type FileLookupFormat = "json" | "yaml";

export interface FormatSpec {
  format: FileLookupFormat;
  label: string;           // "JSON" / "YAML" — capitalized for user-visible text
  typeBadge: string;       // "json" / "yaml" — lowercase badge text
  helpRef: string;         // help section describing this format's lookups
  accept: string;          // accepted file extensions for the drop zone
  fileExtensions: string[]; // filter for managed-file picker
  lookupKey: "jsonFileLookups" | "yamlFileLookups"; // putLookupSettings payload field
  equal: (draft: JSONFileLookupDraft, saved: JSONFileLookupEntry | YAMLFileLookupEntry) => boolean;
  usePreview: typeof usePreviewJSONLookup;
}

export const jsonFormatSpec: FormatSpec = {
  format: "json",
  label: "JSON",
  typeBadge: "json",
  helpRef: "lookups-settings#json-file-lookups",
  accept: ".json",
  fileExtensions: [".json"],
  lookupKey: "jsonFileLookups",
  equal: jsonFileLookupEqual as FormatSpec["equal"],
  usePreview: usePreviewJSONLookup,
};

export const yamlFormatSpec: FormatSpec = {
  format: "yaml",
  label: "YAML",
  typeBadge: "yaml",
  helpRef: "lookups-settings#yaml-file-lookups",
  accept: ".yaml,.yml",
  fileExtensions: [".yaml", ".yml"],
  lookupKey: "yamlFileLookups",
  equal: yamlFileLookupEqual as FormatSpec["equal"],
  usePreview: usePreviewYAMLLookup,
};
