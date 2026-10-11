import { useThemeClass } from "../../hooks/useThemeClass";
import { encode } from "../../api/glid";
import { HelpLink } from "../HelpLink";

interface UsedByStatusProps {
  dark: boolean;
  refs: string[];
  onNavigate?: (ref: string) => void;
  /** Without a navigation target, each reference opens this help section instead. */
  helpRef?: string;
}

export function UsedByStatus({ dark, refs, onNavigate, helpRef }: Readonly<UsedByStatusProps>) {
  const c = useThemeClass(dark);
  if (refs.length === 0) return;
  const renderRef = (ref: string) => {
    if (onNavigate) {
      return (
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
      );
    }
    if (helpRef) {
      return (
        <HelpLink helpRef={helpRef} title={`Used by ${ref}`} className="font-mono text-copper">
          {ref}
        </HelpLink>
      );
    }
    return <>{ref}</>;
  };
  return (
    <span
      className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
    >
      used by:{" "}
      {refs.map((ref, i) => (
        <span key={ref}>
          {i > 0 && ", "}
          {renderRef(ref)}
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
