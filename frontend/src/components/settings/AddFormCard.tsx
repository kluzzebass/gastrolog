import { useThemeClass } from "../../hooks/useThemeClass";
import { PrimaryButton, GhostButton } from "./Buttons";

interface AddFormCardProps {
  dark: boolean;
  onCancel: () => void;
  onCreate: () => void;
  isPending: boolean;
  createDisabled?: boolean;
  typeBadge?: string;
  children: React.ReactNode;
}

export function AddFormCard({
  dark,
  onCancel,
  onCreate,
  isPending,
  createDisabled,
  typeBadge,
  children,
}: Readonly<AddFormCardProps>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`border rounded-lg p-4 ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
    >
      <div className="flex flex-col gap-3">
        {typeBadge && (
          <div className="flex items-center gap-2">
            <span
              className={`px-2 py-0.5 text-[0.75em] font-mono rounded ${c(
                "bg-copper/15 text-copper",
                "bg-copper/15 text-copper",
              )}`}
            >
              {typeBadge}
            </span>
          </div>
        )}
        {children}
        <div className="flex justify-end gap-2 pt-2">
          <GhostButton onClick={onCancel} dark={dark} bordered>
            Cancel
          </GhostButton>
          <PrimaryButton onClick={onCreate} disabled={isPending || createDisabled}>
            {isPending ? "Creating..." : "Create"}
          </PrimaryButton>
        </div>
      </div>
    </div>
  );
}
