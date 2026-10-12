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
 * What a destination candidate is besides a name: a disabled vault has no
 * instance to append to. It remains selectable — a route may be configured
 * ahead of enabling its vault.
 */
export function destinationState(v: Pick<DestinationVault, "enabled">): string | undefined {
  return v.enabled ? undefined : "disabled, not accepting records";
}

export type DestinationVault = { id: string; name: string; type: VaultType; enabled: boolean };
