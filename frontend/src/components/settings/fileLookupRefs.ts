import { encode } from "../../api/glid";
import type { LookupSettings } from "../../api/gen/gastrolog/v1/system_pb";

/**
 * The lookups whose saved configuration references one of these file
 * versions by id — the only sense in which a managed file is "used".
 */
export function fileLookupRefs(versionIds: readonly string[], lookup: LookupSettings | undefined): string[] {
  if (!lookup) return [];
  const ids = new Set(versionIds);
  const refs: string[] = [];
  const fileLookups = [
    ...lookup.mmdbLookups,
    ...lookup.csvLookups,
    ...lookup.jsonFileLookups,
    ...lookup.yamlFileLookups,
  ];
  for (const l of fileLookups) {
    if (l.fileId.length > 0 && ids.has(encode(l.fileId))) refs.push(`lookup ${l.name}`);
  }
  return refs;
}
