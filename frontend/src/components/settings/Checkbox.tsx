import type { ReactNode } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { HelpButton } from "../HelpButton";

interface CheckboxProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  /**
   * Clickable label. Pass a string for the default muted style or a ReactNode
   * for custom layouts (e.g. label + description). The entire label area
   * stays inside the Checkbox's click target so clicking anywhere on it
   * toggles the state, matching the behaviour of native <label>.
   */
  label?: ReactNode;
  /** Additional classes on the outer row — e.g. `flex-1` to grow in a flex parent. */
  className?: string;
  helpTopicId?: string;
  dark: boolean;
}

export function Checkbox({ checked, onChange, label, className, helpTopicId, dark }: Readonly<CheckboxProps>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`flex items-center gap-2 cursor-pointer select-none${className ? " " + className : ""}`}
      onClick={() => onChange(!checked)}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onChange(!checked); } }}
      role="checkbox"
      aria-checked={checked}
      tabIndex={0}
    >
      <button
        type="button"
        className={`w-4 h-4 rounded border flex items-center justify-center shrink-0 transition-colors ${
          checked
            ? "bg-copper border-copper text-text-on-copper"
            : c(
                "border-ink-border bg-ink-well",
                "border-light-border bg-light-well",
              )
        }`}
      >
        {checked && (
          <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
            <path
              d="M2 5L4 7L8 3"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        )}
      </button>
      {typeof label === "string"
        ? label && (
          <span
            className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {label}
          </span>
        )
        : label}
      {helpTopicId && (
        <span onClick={(e) => e.stopPropagation()} onKeyDown={(e) => e.stopPropagation()} role="presentation">
          <HelpButton topicId={helpTopicId} />
        </span>
      )}
    </div>
  );
}
