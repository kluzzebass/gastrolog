import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { Button, DropdownButton } from "./Buttons";

interface DropdownItem {
  value: string;
  label: string;
}

interface SettingsSectionProps {
  addLabel?: string;
  adding: boolean;
  onToggleAdd: () => void;
  isLoading: boolean;
  isEmpty: boolean;
  emptyMessage: string;
  dark: boolean;
  /** Dropdown items for the Add button. When provided, clicking the button opens a menu instead of toggling directly. */
  addOptions?: DropdownItem[];
  /** Called when a dropdown option is selected. Only used when addOptions is provided. */
  onAddSelect?: (value: string) => void;
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
  addOptions,
  onAddSelect,
  disabled,
}: Readonly<SettingsSectionProps>) {
  const c = useThemeClass(dark);

  if (isLoading) {
    return <LoadingPlaceholder dark={dark} />;
  }

  return (
    <div>
      {addLabel && (
        <div className="flex items-center justify-end mb-5">
          {(() => {
            if (adding) return <Button onClick={onToggleAdd}>Cancel</Button>;
            if (addOptions && onAddSelect) {
              return (
                <DropdownButton
                  label={addLabel}
                  items={addOptions}
                  onSelect={onAddSelect}
                  dark={dark}
                />
              );
            }
            return <Button onClick={onToggleAdd}>{addLabel}</Button>;
          })()}
        </div>
      )}

      <div className={`flex flex-col gap-3 ${disabled ? "opacity-50" : ""}`} aria-disabled={disabled || undefined}>
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
