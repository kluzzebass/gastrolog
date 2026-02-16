import { useThemeClass } from "../../hooks/useThemeClass";

interface Store {
  id: string;
  [key: string]: any;
}

interface UsedByStatusProps {
  dark: boolean;
  refs: string[];
}

export function UsedByStatus({ dark, refs }: UsedByStatusProps) {
  const c = useThemeClass(dark);
  if (refs.length === 0) return undefined;
  return (
    <span
      className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
    >
      used by: {refs.join(", ")}
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
