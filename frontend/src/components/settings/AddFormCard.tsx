import { useThemeClass } from "../../hooks/useThemeClass";
import { PrimaryButton, GhostButton } from "./Buttons";

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
          <GhostButton onClick={onCancel} dark={dark} bordered>
            Cancel
          </GhostButton>
          <PrimaryButton onClick={onCreate} disabled={isPending}>
            {isPending ? "Creating..." : "Create"}
          </PrimaryButton>
        </div>
      </div>
    </div>
  );
}
