import { createContext, useContext, useId, useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";

const FormFieldIdContext = createContext<string | undefined>(undefined);

/** Returns the auto-generated id from the enclosing FormField, if any. */
function useFormFieldId() {
  return useContext(FormFieldIdContext);
}

interface FormFieldProps {
  label: string;
  description?: React.ReactNode;
  dark: boolean;
  children: React.ReactNode;
}

export function FormField({
  label,
  description,
  dark,
  children,
}: Readonly<FormFieldProps>) {
  const id = useId();
  const c = useThemeClass(dark);
  return (
    <FormFieldIdContext.Provider value={id}>
      <div className="flex flex-col gap-1">
        <label
          htmlFor={id}
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {label}
        </label>
        {children}
        {description && (
          <div
            className={`text-[0.85em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {description}
          </div>
        )}
      </div>
    </FormFieldIdContext.Provider>
  );
}

export function ExampleValues({
  examples,
  value,
  onChange,
  dark,
}: Readonly<{
  examples?: string[];
  value: string;
  onChange: (v: string) => void;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  if (!examples?.length || value) return null;
  return (
    <div
      className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
    >
      {examples.map((ex, i) => (
        <span key={ex}>
          {i > 0 && ", "}
          <button
            type="button"
            onClick={() => onChange(ex)}
            className={`font-mono cursor-pointer transition-colors ${c(
              "hover:text-copper",
              "hover:text-copper",
            )}`}
          >
            {ex}
          </button>
        </span>
      ))}
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
  examples?: string[];
}

export function TextInput({
  value,
  onChange,
  placeholder,
  dark,
  disabled,
  mono,
  examples,
}: Readonly<TextInputProps>) {
  const id = useFormFieldId();
  const c = useThemeClass(dark);
  return (
    <>
      <input
        id={id}
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        disabled={disabled}
        className={`px-2.5 py-1.5 text-[0.85em] border rounded focus:outline-none transition-colors ${
          mono ? "font-mono" : ""
        } ${c(
          "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
          "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
        )} ${disabled ? "opacity-50 cursor-not-allowed" : ""}`}
      />
      <ExampleValues examples={examples} value={value} onChange={onChange} dark={dark} />
    </>
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
}: Readonly<SelectInputProps>) {
  const id = useFormFieldId();
  const c = useThemeClass(dark);
  return (
    <select
      id={id}
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
  examples?: string[];
}

export function NumberInput({
  value,
  onChange,
  placeholder,
  dark,
  disabled,
  min,
  examples,
}: Readonly<NumberInputProps>) {
  const id = useFormFieldId();
  const c = useThemeClass(dark);
  return (
    <>
      <input
        id={id}
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
          "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
          "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
        )} ${disabled ? "opacity-50 cursor-not-allowed" : ""}`}
      />
      <ExampleValues examples={examples} value={value} onChange={onChange} dark={dark} />
    </>
  );
}

interface SpinnerInputProps {
  value: string;
  onChange: (v: string) => void;
  dark: boolean;
  disabled?: boolean;
  min?: number;
  max?: number;
  skip?: number[]; // values to skip when stepping (e.g. [2] to skip RF=2)
}

export function SpinnerInput({
  value,
  onChange,
  dark,
  disabled,
  min = 1,
  max,
  skip = [],
}: Readonly<SpinnerInputProps>) {
  const id = useFormFieldId();
  const c = useThemeClass(dark);
  const n = parseInt(value, 10) || min;

  const step = (dir: 1 | -1) => {
    let next = n + dir;
    while (skip.includes(next)) next += dir;
    if (next < min) return;
    if (max !== undefined && next > max) return;
    onChange(String(next));
  };

  const btnClass = `px-2 py-1 text-[0.85em] border transition-colors select-none ${c(
    "bg-ink-surface border-ink-border text-text-muted hover:bg-ink-hover active:bg-ink-pressed",
    "bg-light-surface border-light-border text-light-text-muted hover:bg-light-hover active:bg-light-pressed",
  )} ${disabled ? "opacity-50 cursor-not-allowed" : "cursor-pointer"}`;

  return (
    <div className="flex items-center">
      <button
        type="button"
        className={`${btnClass} rounded-l border-r-0`}
        onClick={() => step(-1)}
        disabled={disabled || n <= min}
        aria-label="Decrease"
      >
        {"\u25C0"}
      </button>
      <input
        id={id}
        type="text"
        inputMode="numeric"
        value={value}
        onChange={(e) => {
          const v = e.target.value;
          if (v === "" || /^\d+$/.test(v)) {
            const parsed = parseInt(v, 10);
            if (v !== "" && parsed < min) return;
            if (v !== "" && max !== undefined && parsed > max) return;
            if (v !== "" && skip.includes(parsed)) return;
            onChange(v);
          }
        }}
        disabled={disabled}
        className={`w-12 text-center px-1 py-1.5 text-[0.85em] font-mono border-y border-x-0 focus:outline-none transition-colors ${c(
          "bg-ink-surface border-ink-border text-text-bright focus:border-copper-dim",
          "bg-light-surface border-light-border text-light-text-bright focus:border-copper",
        )} ${disabled ? "opacity-50 cursor-not-allowed" : ""}`}
      />
      <button
        type="button"
        className={`${btnClass} rounded-r border-l-0`}
        onClick={() => step(1)}
        disabled={disabled || (max !== undefined && n >= max)}
        aria-label="Increase"
      >
        {"\u25B6"}
      </button>
    </div>
  );
}

interface TextAreaProps {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  dark: boolean;
  disabled?: boolean;
  rows?: number;
  className?: string;
}

export function TextArea({
  value,
  onChange,
  placeholder,
  dark,
  disabled,
  rows = 4,
  className,
}: Readonly<TextAreaProps>) {
  const id = useFormFieldId();
  const c = useThemeClass(dark);
  return (
    <textarea
      id={id}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      disabled={disabled}
      rows={rows}
      className={`px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors resize-y ${c(
        "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
        "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
      )} ${disabled ? "opacity-50 cursor-not-allowed" : ""} ${className ?? ""}`}
    />
  );
}

interface ParamsEditorProps {
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}

export function ParamsEditor({ params, onChange, dark }: Readonly<ParamsEditorProps>) {
  const c = useThemeClass(dark);
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
            className={`text-[0.8em] font-mono w-28 shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}
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
            className={`px-3 py-1 text-[0.8em] rounded border transition-colors ${c(
              "border-ink-border text-text-muted hover:text-severity-error hover:border-severity-error hover:bg-ink-hover",
              "border-light-border text-light-text-muted hover:text-severity-error hover:border-severity-error hover:bg-light-hover",
            )}`}
          >
            Remove
          </button>
        </div>
      ))}
      <div className="flex gap-1.5 items-center">
        <input
          type="text"
          value={newKey}
          onChange={(e) => setNewKey(e.target.value)}
          placeholder="key"
          aria-label="Parameter key"
          className={`w-28 shrink-0 px-2 py-1 text-[0.8em] font-mono border rounded focus:outline-none ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
          )}`}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
        />
        <input
          type="text"
          value={newValue}
          onChange={(e) => setNewValue(e.target.value)}
          placeholder="value"
          aria-label="Parameter value"
          className={`flex-1 px-2 py-1 text-[0.8em] font-mono border rounded focus:outline-none ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
          )}`}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
        />
        <button
          onClick={handleAdd}
          className={`px-3 py-1 text-[0.8em] rounded border transition-colors ${c(
            "border-ink-border text-text-muted hover:text-copper hover:border-copper-dim hover:bg-ink-hover",
            "border-light-border text-light-text-muted hover:text-copper hover:border-copper hover:bg-light-hover",
          )}`}
        >
          Add
        </button>
      </div>
    </div>
  );
}
