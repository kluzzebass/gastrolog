/** Canonical vault write-model values (matches backend system.VaultWriteModel). */
export type WriteModel = "chunk_append" | "sequenced";

const WRITE_MODEL_OPTIONS: { value: WriteModel; label: string }[] = [
  { value: "chunk_append", label: "Chunk append (default)" },
  { value: "sequenced", label: "Sequenced" },
];

/** Resolves config/CLI write_model strings to a canonical select value. */
export function normalizeWriteModel(raw: string | undefined): WriteModel {
  return raw === "sequenced" ? "sequenced" : "chunk_append";
}

export function usesSequencedWriteModel(raw: string | undefined): boolean {
  return normalizeWriteModel(raw) === "sequenced";
}

export function writeModelSelectOptions(): { value: WriteModel; label: string }[] {
  return WRITE_MODEL_OPTIONS;
}
