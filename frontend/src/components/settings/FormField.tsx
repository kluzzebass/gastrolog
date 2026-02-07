import { useState } from "react";

interface FormFieldProps {
  label: string;
  description?: string;
  dark: boolean;
  children: React.ReactNode;
}

export function FormField({
  label,
  description,
  dark,
  children,
}: FormFieldProps) {
  const c = (d: string, l: string) => (dark ? d : l);
  return (
    <div className="flex flex-col gap-1">
      <label
        className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {label}
      </label>
      {children}
      {description && (
        <p
          className={`text-[0.7em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          {description}
        </p>
      )}
    </div>
  );
}

interface TextInputProps {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  dark: boolean;
  disabled?: boolean;
  mono?: boolean;
}

export function TextInput({
  value,
  onChange,
  placeholder,
  dark,
  disabled,
  mono,
}: TextInputProps) {
  const c = (d: string, l: string) => (dark ? d : l);
  return (
    <input
      type="text"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      disabled={disabled}
      className={`px-2.5 py-1.5 text-[0.85em] border rounded focus:outline-none transition-colors ${
        mono ? "font-mono" : ""
      } ${c(
        "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
        "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
      )} ${disabled ? "opacity-50 cursor-not-allowed" : ""}`}
    />
  );
}

interface SelectInputProps {
  value: string;
  onChange: (v: string) => void;
  options: { value: string; label: string }[];
  dark: boolean;
  disabled?: boolean;
}

export function SelectInput({
  value,
  onChange,
  options,
  dark,
  disabled,
}: SelectInputProps) {
  const c = (d: string, l: string) => (dark ? d : l);
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      disabled={disabled}
      className={`px-2.5 py-1.5 text-[0.85em] border rounded focus:outline-none transition-colors ${c(
        "bg-ink-surface border-ink-border text-text-bright focus:border-copper-dim",
        "bg-light-surface border-light-border text-light-text-bright focus:border-copper",
      )} ${disabled ? "opacity-50 cursor-not-allowed" : ""}`}
    >
      {options.map((o) => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  );
}

interface NumberInputProps {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  dark: boolean;
  disabled?: boolean;
  min?: number;
}

export function NumberInput({
  value,
  onChange,
  placeholder,
  dark,
  disabled,
  min,
}: NumberInputProps) {
  const c = (d: string, l: string) => (dark ? d : l);
  return (
    <input
      type="text"
      inputMode="numeric"
      value={value}
      onChange={(e) => {
        const v = e.target.value;
        if (v === "" || /^\d+$/.test(v)) {
          if (min !== undefined && v !== "" && parseInt(v, 10) < min) return;
          onChange(v);
        }
      }}
      placeholder={placeholder}
      disabled={disabled}
      className={`px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors ${c(
        "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
        "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
      )} ${disabled ? "opacity-50 cursor-not-allowed" : ""}`}
    />
  );
}

interface ParamsEditorProps {
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}

export function ParamsEditor({ params, onChange, dark }: ParamsEditorProps) {
  const c = (d: string, l: string) => (dark ? d : l);
  const [newKey, setNewKey] = useState("");
  const [newValue, setNewValue] = useState("");

  const entries = Object.entries(params);

  const handleRemove = (key: string) => {
    const next = { ...params };
    delete next[key];
    onChange(next);
  };

  const handleChange = (key: string, val: string) => {
    onChange({ ...params, [key]: val });
  };

  const handleAdd = () => {
    if (!newKey.trim()) return;
    onChange({ ...params, [newKey.trim()]: newValue });
    setNewKey("");
    setNewValue("");
  };

  return (
    <div className="flex flex-col gap-1.5">
      {entries.map(([key, val]) => (
        <div key={key} className="flex gap-1.5 items-center">
          <span
            className={`text-[0.8em] font-mono min-w-[80px] shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {key}
          </span>
          <input
            type="text"
            value={val}
            onChange={(e) => handleChange(key, e.target.value)}
            className={`flex-1 px-2 py-1 text-[0.8em] font-mono border rounded focus:outline-none ${c(
              "bg-ink-surface border-ink-border text-text-bright focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright focus:border-copper",
            )}`}
          />
          <button
            onClick={() => handleRemove(key)}
            className={`px-1.5 py-1 text-[0.75em] rounded transition-colors ${c(
              "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
              "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
            )}`}
          >
            &times;
          </button>
        </div>
      ))}
      <div className="flex gap-1.5 items-center">
        <input
          type="text"
          value={newKey}
          onChange={(e) => setNewKey(e.target.value)}
          placeholder="key"
          className={`min-w-[80px] w-[80px] shrink-0 px-2 py-1 text-[0.8em] font-mono border rounded focus:outline-none ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
          )}`}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
        />
        <input
          type="text"
          value={newValue}
          onChange={(e) => setNewValue(e.target.value)}
          placeholder="value"
          className={`flex-1 px-2 py-1 text-[0.8em] font-mono border rounded focus:outline-none ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
          )}`}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
        />
        <button
          onClick={handleAdd}
          className={`px-1.5 py-1 text-[0.75em] rounded transition-colors ${c(
            "text-text-ghost hover:text-copper hover:bg-ink-hover",
            "text-light-text-ghost hover:text-copper hover:bg-light-hover",
          )}`}
        >
          +
        </button>
      </div>
    </div>
  );
}
