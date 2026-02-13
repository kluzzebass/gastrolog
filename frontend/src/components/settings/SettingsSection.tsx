import { useThemeClass } from "../../hooks/useThemeClass";

interface SettingsSectionProps {
  title: string;
  addLabel: string;
  adding: boolean;
  onToggleAdd: () => void;
  isLoading: boolean;
  isEmpty: boolean;
  emptyMessage: string;
  dark: boolean;
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
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          {title}
        </h2>
        <button
          onClick={onToggleAdd}
          className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors"
        >
          {adding ? "Cancel" : addLabel}
        </button>
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
