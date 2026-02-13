import { forwardRef } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";

interface AuthFormFieldProps {
  label: string;
  type: string;
  value: string;
  onChange: (value: string) => void;
  error?: boolean;
  disabled?: boolean;
  placeholder?: string;
  autoComplete?: string;
  dark: boolean;
}

export const AuthFormField = forwardRef<HTMLInputElement, AuthFormFieldProps>(
  function AuthFormField(
    {
      label,
      type,
      value,
      onChange,
      error,
      disabled,
      placeholder,
      autoComplete,
      dark,
    },
    ref,
  ) {
    const c = useThemeClass(dark);
    return (
      <div className="flex flex-col gap-1.5">
        <label
          className={`text-[0.78em] font-medium tracking-wide uppercase ${
            error
              ? "text-severity-error"
              : c("text-text-muted", "text-light-text-muted")
          }`}
        >
          {label}
        </label>
        <input
          ref={ref}
          type={type}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          autoComplete={autoComplete}
          disabled={disabled}
          className={`px-3 py-2 text-[0.9em] border rounded focus:outline-none transition-colors ${
            error
              ? "border-severity-error"
              : c(
                  "border-ink-border focus:border-copper-dim",
                  "border-light-border focus:border-copper",
                )
          } ${c(
            "bg-ink text-text-bright placeholder:text-text-ghost",
            "bg-light-bg text-light-text-bright placeholder:text-light-text-ghost",
          )} ${disabled ? "opacity-50 cursor-not-allowed" : ""}`}
        />
      </div>
    );
  },
);
