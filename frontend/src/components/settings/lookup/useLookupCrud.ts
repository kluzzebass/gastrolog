/**
 * Shared CRUD scaffolding for lookup section cards.
 * Extracted from HttpSection, JsonSection, MmdbSection, CsvSection.
 */
import { useState } from "react";
import { usePutSettings, useDeleteLookup } from "../../../api/hooks/useSettings";
import { useToast } from "../../Toast";

interface LookupCrudOptions<T, S> {
  /** Current draft lookups (may be modified by the user). */
  lookups: T[];
  /** Saved lookups from the last successful load (for dirty detection). */
  savedLookups: S[];
  /** Serialize drafts to the wire format for putSettings. */
  serialize: (items: T[]) => unknown[];
  /** Compare a draft against a saved entry for equality (dirty detection). */
  equal: (draft: T, saved: S) => boolean;
  /** The lookup key in the settings payload (e.g. "httpLookups"). */
  lookupKey: string;
  /** Display label for toasts (e.g. "HTTP"). */
  typeLabel: string;
  /** Get the display name from a draft item. */
  getName: (item: T) => string;
  /** Callback after successful delete. */
  onDelete: (i: number) => void;
}

export function useLookupCrud<T, S>(opts: LookupCrudOptions<T, S>) {
  const {
    lookups, savedLookups, serialize, equal,
    lookupKey, typeLabel, getName, onDelete,
  } = opts;

  const putConfig = usePutSettings();
  const deleteLookup = useDeleteLookup();
  const { addToast } = useToast();
  const [justSaved, setJustSaved] = useState(false);

  const isDirty = (i: number): boolean => {
    if (justSaved) return false;
    const saved = savedLookups[i];
    if (!saved) return true;
    return !equal(lookups[i]!, saved);
  };

  const save = async (i: number) => {
    const draft = lookups[i]!;
    try {
      await putConfig.mutateAsync({ lookup: { [lookupKey]: serialize(lookups) } });
      setJustSaved(true);
      requestAnimationFrame(() => setJustSaved(false));
      addToast(`${typeLabel} lookup "${getName(draft)}" saved`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : `Failed to save ${typeLabel} lookup`, "error");
    }
  };

  const handleDelete = async (i: number) => {
    const item = lookups[i];
    const name = item ? getName(item) : `${typeLabel} Lookup ${i + 1}`;
    try {
      await deleteLookup.mutateAsync(name);
      onDelete(i);
      addToast(`"${name}" deleted`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : `Failed to delete ${typeLabel} lookup`, "error");
    }
  };

  return { isDirty, save, handleDelete, putConfig };
}
