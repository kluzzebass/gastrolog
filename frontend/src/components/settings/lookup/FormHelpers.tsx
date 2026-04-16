import { useState } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import type { LookupParamDraft } from "./types";

export function StringListEditor({
  values,
  onChange,
  placeholder,
  dark,
}: Readonly<{
  values: string[];
  onChange: (v: string[]) => void;
  placeholder: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const [draft, setDraft] = useState("");

  const handleAdd = () => {
    if (!draft.trim()) return;
    onChange([...values, draft.trim()]);
    setDraft("");
  };

  return (
    <div className="flex flex-col gap-1.5">
      {values.map((v, i) => (
        <div key={i} className="flex gap-1.5 items-center">
          <input
            type="text"
            value={v}
            onChange={(e) => {
              const next = [...values];
              next[i] = e.target.value;
              onChange(next);
            }}
            className={`flex-1 px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none ${c(
              "bg-ink-surface border-ink-border text-text-bright focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright focus:border-copper",
            )}`}
          />
          <button
            onClick={() => onChange(values.filter((_, j) => j !== i))}
            className={`px-2 py-1.5 text-[0.8em] rounded border transition-colors ${c(
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
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder={placeholder}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
          className={`flex-1 px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none ${c(
            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
          )}`}
        />
        <button
          onClick={handleAdd}
          className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
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

export function ParameterListEditor({
  values,
  onChange,
  dark,
}: Readonly<{
  values: LookupParamDraft[];
  onChange: (v: LookupParamDraft[]) => void;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);

  const swap = (a: number, b: number) => {
    const next = [...values];
    [next[a], next[b]] = [next[b]!, next[a]!];
    onChange(next);
  };

  return (
    <div className="flex flex-col gap-1.5">
      {values.map((p, i) => (
        <div key={i} className="flex gap-1.5 items-center">
          <div className="flex flex-col gap-0.5">
            <button
              disabled={i === 0}
              onClick={() => swap(i, i - 1)}
              className={`px-1 py-0 text-[0.7em] leading-none rounded transition-colors disabled:opacity-20 ${c(
                "text-text-muted hover:text-copper",
                "text-light-text-muted hover:text-copper",
              )}`}
            >
              ▲
            </button>
            <button
              disabled={i === values.length - 1}
              onClick={() => swap(i, i + 1)}
              className={`px-1 py-0 text-[0.7em] leading-none rounded transition-colors disabled:opacity-20 ${c(
                "text-text-muted hover:text-copper",
                "text-light-text-muted hover:text-copper",
              )}`}
            >
              ▼
            </button>
          </div>
          <input
            type="text"
            value={p.name}
            onChange={(e) => {
              const next = [...values];
              next[i] = { ...next[i]!, name: e.target.value };
              onChange(next);
            }}
            placeholder="name"
            className={`w-32 px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none ${c(
              "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
            )}`}
          />
          <input
            type="text"
            value={p.description}
            onChange={(e) => {
              const next = [...values];
              next[i] = { ...next[i]!, description: e.target.value };
              onChange(next);
            }}
            placeholder="description"
            className={`flex-1 px-2.5 py-1.5 text-[0.85em] border rounded focus:outline-none ${c(
              "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
            )}`}
          />
          <button
            onClick={() => onChange(values.filter((_, j) => j !== i))}
            className={`px-2 py-1.5 text-[0.8em] rounded border transition-colors ${c(
              "border-ink-border text-text-muted hover:text-severity-error hover:border-severity-error hover:bg-ink-hover",
              "border-light-border text-light-text-muted hover:text-severity-error hover:border-severity-error hover:bg-light-hover",
            )}`}
          >
            Remove
          </button>
        </div>
      ))}
      <button
        onClick={() => onChange([...values, { name: "", description: "" }])}
        className={`self-start px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
          "border-ink-border text-text-muted hover:text-copper hover:border-copper-dim hover:bg-ink-hover",
          "border-light-border text-light-text-muted hover:text-copper hover:border-copper hover:bg-light-hover",
        )}`}
      >
        + Add Parameter
      </button>
    </div>
  );
}

export function PasswordInput({
  value,
  onChange,
  placeholder,
  dark,
}: Readonly<{
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  return (
    <input
      type="password"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      autoComplete="off"
      className={`px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors ${c(
        "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-muted focus:border-copper-dim",
        "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-muted focus:border-copper",
      )}`}
    />
  );
}
