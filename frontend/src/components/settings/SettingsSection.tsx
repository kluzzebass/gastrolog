import { useThemeClass } from "../../hooks/useThemeClass";
import { PrimaryButton } from "./Buttons";

interface SettingsSectionProps {
  addLabel?: string;
  adding: boolean;
  onToggleAdd: () => void;
  isLoading: boolean;
  isEmpty: boolean;
  emptyMessage: string;
  dark: boolean;
  /** Replaces the default add/cancel button when provided. */
  addSlot?: React.ReactNode;
  /** When true, content is visually disabled (greyed out, no interaction). */
  disabled?: boolean;
  children: React.ReactNode;
}

export function SettingsSection({
  addLabel,
  adding,
  onToggleAdd,
  isLoading,
  isEmpty,
  emptyMessage,
  dark,
  children,
  addSlot,
  disabled,
}: Readonly<SettingsSectionProps>) {
  const c = useThemeClass(dark);

  if (isLoading) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading...
      </div>
    );
  }

  return (
    <div>
      {addLabel && (
        <div className="flex items-center justify-end mb-5">
          {addSlot || (
            <PrimaryButton onClick={onToggleAdd}>
              {adding ? "Cancel" : addLabel}
            </PrimaryButton>
          )}
        </div>
      )}

      <div className={`flex flex-col gap-3 ${disabled ? "opacity-50 pointer-events-none" : ""}`}>
        {children}

        {isEmpty && !adding && (
          <div
            className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            {emptyMessage}
          </div>
        )}
      </div>
    </div>
  );
}
