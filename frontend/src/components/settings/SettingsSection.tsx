import { useThemeClass } from "../../hooks/useThemeClass";
import { PrimaryButton } from "./Buttons";
import { HelpButton } from "../HelpButton";

interface SettingsSectionProps {
  title: string;
  addLabel: string;
  adding: boolean;
  onToggleAdd: () => void;
  isLoading: boolean;
  isEmpty: boolean;
  emptyMessage: string;
  dark: boolean;
  /** Replaces the default add/cancel button when provided. */
  addSlot?: React.ReactNode;
  helpTopicId?: string;
  children: React.ReactNode;
}

export function SettingsSection({
  title,
  addLabel,
  adding,
  onToggleAdd,
  isLoading,
  isEmpty,
  emptyMessage,
  dark,
  children,
  addSlot,
  helpTopicId,
}: SettingsSectionProps) {
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
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-2">
          <h2
            className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            {title}
          </h2>
          {helpTopicId && <HelpButton topicId={helpTopicId} />}
        </div>
        {addSlot || (
          <PrimaryButton onClick={onToggleAdd}>
            {adding ? "Cancel" : addLabel}
          </PrimaryButton>
        )}
      </div>

      <div className="flex flex-col gap-3">
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
