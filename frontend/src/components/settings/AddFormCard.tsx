import { useThemeClass } from "../../hooks/useThemeClass";

interface AddFormCardProps {
  dark: boolean;
  onCancel: () => void;
  onCreate: () => void;
  isPending: boolean;
  children: React.ReactNode;
}

export function AddFormCard({
  dark,
  onCancel,
  onCreate,
  isPending,
  children,
}: AddFormCardProps) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`border rounded-lg p-4 ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
    >
      <div className="flex flex-col gap-3">
        {children}
        <div className="flex justify-end gap-2 pt-2">
          <button
            onClick={onCancel}
            className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
              "border-ink-border text-text-muted hover:bg-ink-hover",
              "border-light-border text-light-text-muted hover:bg-light-hover",
            )}`}
          >
            Cancel
          </button>
          <button
            onClick={onCreate}
            disabled={isPending}
            className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
          >
            {isPending ? "Creating..." : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}
