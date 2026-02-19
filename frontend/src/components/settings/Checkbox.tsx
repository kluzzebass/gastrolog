import { useThemeClass } from "../../hooks/useThemeClass";

interface CheckboxProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label?: string;
  dark: boolean;
}

export function Checkbox({ checked, onChange, label, dark }: Readonly<CheckboxProps>) {
  const c = useThemeClass(dark);
  return (
    <div
      className="flex items-center gap-2 cursor-pointer select-none"
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
            ? "bg-copper border-copper text-white"
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
      {label && (
        <span
          className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {label}
        </span>
      )}
    </div>
  );
}
