import { useThemeClass } from "../../hooks/useThemeClass";
import { encode } from "../../api/glid";

interface UsedByStatusProps {
  dark: boolean;
  refs: string[];
  onNavigate?: (ref: string) => void;
}

export function UsedByStatus({ dark, refs, onNavigate }: Readonly<UsedByStatusProps>) {
  const c = useThemeClass(dark);
  if (refs.length === 0) return;
  return (
    <span
      className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      used by:{" "}
      {refs.map((ref, i) => (
        <span key={ref}>
          {i > 0 && ", "}
          {onNavigate ? (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onNavigate(ref);
              }}
              className={`underline decoration-dotted underline-offset-2 transition-colors ${c(
                "text-text-muted hover:text-copper",
                "text-light-text-muted hover:text-copper",
              )}`}
            >
              {ref}
            </button>
          ) : (
            ref
          )}
        </span>
      ))}
    </span>
  );
}

interface Route {
  id: Uint8Array;
  name: string;
  filterId: Uint8Array;
  destinations: { vaultId: Uint8Array }[];
}

export function routeRefsForFilter(
  routes: Route[],
  filterId: string,
): string[] {
  return routes
    .filter((r) => encode(r.filterId) === filterId)
    .map((r) => r.name || encode(r.id));
}

interface Tier {
  id: Uint8Array;
  name: string;
  vaultId: Uint8Array;
  position: number;
  rotationPolicyId: Uint8Array;
  retentionRules: { retentionPolicyId: Uint8Array }[];
}

interface VaultRef {
  id: Uint8Array;
  name: string;
}

function tierLabel(tier: Tier, vaults: VaultRef[]): string | null {
  const vaultId = encode(tier.vaultId);
  const vault = vaults.find((v) => encode(v.id) === vaultId);
  if (!vault) return null;
  return `${vault.name || vaultId}/tier ${String(tier.position + 1)}`;
}

export function tierRefsForRotationPolicy(
  tiers: Tier[],
  rotationPolicyId: string,
  vaults: VaultRef[] = [],
): string[] {
  return tiers
    .filter((t) => encode(t.rotationPolicyId) === rotationPolicyId)
    .map((t) => tierLabel(t, vaults))
    .filter((label): label is string => label !== null);
}

export function tierRuleRefsFor(
  tiers: Tier[],
  retentionPolicyId: string,
  vaults: VaultRef[] = [],
): string[] {
  return tiers
    .filter((t) =>
      t.retentionRules.some(
        (r) => encode(r.retentionPolicyId) === retentionPolicyId,
      ),
    )
    .map((t) => tierLabel(t, vaults))
    .filter((label): label is string => label !== null);
}
