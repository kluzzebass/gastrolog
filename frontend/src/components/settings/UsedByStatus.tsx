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
      className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
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

interface VaultRef {
  id: Uint8Array;
  name: string;
  rotationPolicyId: Uint8Array;
  retentionRules: { retentionPolicyId: Uint8Array }[];
}

function vaultLabel(v: VaultRef): string {
  return v.name || encode(v.id);
}

export function vaultRefsForRotationPolicy(
  rotationPolicyId: string,
  vaults: VaultRef[] = [],
): string[] {
  return vaults
    .filter((v) => encode(v.rotationPolicyId) === rotationPolicyId)
    .map(vaultLabel);
}

export function vaultRefsForRetentionPolicy(
  retentionPolicyId: string,
  vaults: VaultRef[] = [],
): string[] {
  return vaults
    .filter((v) =>
      v.retentionRules.some(
        (r) => encode(r.retentionPolicyId) === retentionPolicyId,
      ),
    )
    .map(vaultLabel);
}
