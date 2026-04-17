// Thin wrapper that binds the shared FileLookupSection components to the
// YAML format spec. YAML parses into the same tree JSON does, so the
// jq/key/value-column UI is identical — only the file extensions, preview
// hook, payload key, and type badge differ.

import { FileLookupAddForm, FileLookupCards, yamlFormatSpec } from "./FileLookupSection";
import type { YAMLFileLookupDraft, LookupSectionProps } from "./types";
import type { YAMLFileLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";

export { serializeFileLookups as serializeYamlLookups } from "./FileLookupSection";

export function YamlAddForm(props: LookupSectionProps & {
  onCreated: (draft: YAMLFileLookupDraft) => void;
  onCancel: () => void;
  existingLookups: YAMLFileLookupDraft[];
  namePlaceholder: string;
}) {
  return <FileLookupAddForm {...props} spec={yamlFormatSpec} />;
}

export function YamlCards(props: LookupSectionProps & {
  lookups: YAMLFileLookupDraft[];
  savedLookups: YAMLFileLookupEntry[];
  onUpdate: (i: number, patch: Partial<YAMLFileLookupDraft>) => void;
  onDelete: (i: number) => void;
}) {
  return <FileLookupCards {...props} spec={yamlFormatSpec} />;
}
