// Thin wrapper that binds the shared FileLookupSection components to the
// JSON format spec. The section UI, preview panel, and jq/key/value-column
// logic all live in FileLookupSection.tsx — only the format-specific bits
// (preview hook, file extensions, payload key, type badge) vary.

import { FileLookupAddForm, FileLookupCards, jsonFormatSpec } from "./FileLookupSection";
import type { JSONFileLookupDraft, LookupSectionProps } from "./types";
import type { JSONFileLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";

export { serializeFileLookups as serializeJsonLookups } from "./FileLookupSection";

export function JsonAddForm(props: LookupSectionProps & {
  onCreated: (draft: JSONFileLookupDraft) => void;
  onCancel: () => void;
  existingLookups: JSONFileLookupDraft[];
  namePlaceholder: string;
}) {
  return <FileLookupAddForm {...props} spec={jsonFormatSpec} />;
}

export function JsonCards(props: LookupSectionProps & {
  lookups: JSONFileLookupDraft[];
  savedLookups: JSONFileLookupEntry[];
  onUpdate: (i: number, patch: Partial<JSONFileLookupDraft>) => void;
  onDelete: (i: number) => void;
}) {
  return <FileLookupCards {...props} spec={jsonFormatSpec} />;
}
