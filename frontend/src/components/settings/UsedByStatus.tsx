import { useThemeClass } from "../../hooks/useThemeClass";

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
  id: string;
  name: string;
  filterId: string;
  destinations: { vaultId: string }[];
}

export function routeRefsForFilter(
  routes: Route[],
  filterId: string,
): string[] {
  return routes
    .filter((r) => r.filterId === filterId)
    .map((r) => r.name || r.id);
}

interface Tier {
  id: string;
  name: string;
  vaultId: string;
  position: number;
  rotationPolicyId: string;
  retentionRules: { retentionPolicyId: string }[];
}

interface VaultRef {
  id: string;
  name: string;
}

function tierLabel(tier: Tier, vaults: VaultRef[]): string | null {
  const vault = vaults.find((v) => v.id === tier.vaultId);
  if (!vault) return null;
  return `${vault.name || vault.id}/tier ${String(tier.position + 1)}`;
}

export function tierRefsForRotationPolicy(
  tiers: Tier[],
  rotationPolicyId: string,
  vaults: VaultRef[] = [],
): string[] {
  return tiers
    .filter((t) => t.rotationPolicyId === rotationPolicyId)
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
        (r) => r.retentionPolicyId === retentionPolicyId,
      ),
    )
    .map((t) => tierLabel(t, vaults))
    .filter((label): label is string => label !== null);
}
