import { useThemeClass } from "../../hooks/useThemeClass";

interface Store {
  id: string;
  [key: string]: any;
}

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

export function refsFor(
  stores: Store[],
  field: string,
  id: string,
): string[] {
  return stores.filter((s) => s[field] === id).map((s) => s.name || s.id);
}

export function ruleRefsFor(
  stores: Store[],
  retentionPolicyId: string,
): string[] {
  return stores
    .filter((s) =>
      (s.retentionRules ?? []).some(
        (b: { retentionPolicyId: string }) =>
          b.retentionPolicyId === retentionPolicyId,
      ),
    )
    .map((s) => s.name || s.id);
}
