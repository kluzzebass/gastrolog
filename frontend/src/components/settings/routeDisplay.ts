import { VaultType } from "../../api/gen/gastrolog/v1/system_pb";

/**
 * The expression as the card header shows it: a `_vault` comparison against a
 * configured vault's ID reads with the vault's name in its place. An ID no
 * vault carries stays as written.
 */
export function displayExpression(expr: string, vaults: ReadonlyArray<{ id: string; name: string }>): string {
  return expr.replace(/(_vault\s*(?:==?|!=)\s*")([^"]*)(")/g, (match, open: string, id: string, close: string) => {
    const vault = vaults.find((v) => v.id === id);
    return vault?.name ? `${open}${vault.name}${close}` : match;
  });
}

/**
 * What a destination candidate is besides a name: a JSONL sink cannot be
 * searched, and a disabled vault has no instance to append to. Both remain
 * selectable — a route may be configured ahead of enabling its vault.
 */
export function destinationState(v: Pick<DestinationVault, "type" | "enabled">): string | undefined {
  const notes: string[] = [];
  if (v.type === VaultType.JSONL) notes.push("JSONL sink, write-only");
  if (!v.enabled) notes.push("disabled, not accepting records");
  return notes.length > 0 ? notes.join("; ") : undefined;
}

export type DestinationVault = { id: string; name: string; type: VaultType; enabled: boolean };
