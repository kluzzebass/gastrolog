import { useHelp } from "../../hooks/useHelp";
import { useThemeClass } from "../../hooks/useThemeClass";
import { SelectInput } from "./FormField";

/**
 * A picker candidate. An option with an ineligibleReason is still listed —
 * disabled, its reason beside its label — so the operator sees everything
 * that exists and why it does not qualify.
 */
export interface EligibilityOption {
  value: string;
  label: string;
  ineligibleReason?: string;
}

function ineligibleLabel(o: EligibilityOption): string {
  return o.ineligibleReason ? `${o.label} — ${o.ineligibleReason}` : o.label;
}

/**
 * A select whose candidates are filtered by an eligibility rule. When the
 * rule leaves nothing eligible, or the current value does not qualify, a
 * one-line note names the cause and links to the help section that states
 * the rule.
 */
export function EligibilitySelect({
  value,
  onChange,
  options,
  placeholder,
  emptyMessage,
  helpRef,
  dark,
}: Readonly<{
  value: string;
  onChange: (v: string) => void;
  options: EligibilityOption[];
  placeholder?: string;
  emptyMessage?: string;
  // Help topic ID, optionally with a "#section" anchor.
  helpRef: string;
  dark: boolean;
}>) {
  const selectOptions = options.map((o) => ({
    value: o.value,
    label: ineligibleLabel(o),
    disabled: o.ineligibleReason !== undefined,
  }));
  if (placeholder !== undefined) {
    selectOptions.unshift({ value: "", label: placeholder, disabled: false });
  }

  const selected = options.find((o) => o.value === value);
  let note: string | undefined;
  if (selected?.ineligibleReason) {
    note = `${selected.label} is not eligible — ${selected.ineligibleReason}.`;
  } else if (emptyMessage && !options.some((o) => !o.ineligibleReason)) {
    note = emptyMessage;
  }

  return (
    <>
      <SelectInput value={value} onChange={onChange} options={selectOptions} dark={dark} />
      {note && <EligibilityNote note={note} helpRef={helpRef} dark={dark} />}
    </>
  );
}

function EligibilityNote({ note, helpRef, dark }: Readonly<{ note: string; helpRef: string; dark: boolean }>) {
  const { openHelp } = useHelp();
  const c = useThemeClass(dark);
  return (
    <div role="note" className={`text-[0.85em] leading-relaxed ${c("text-text-normal", "text-light-text-normal")}`}>
      {note}{" "}
      <button
        type="button"
        onClick={() => openHelp(helpRef)}
        className="text-copper hover:underline cursor-pointer"
      >
        Eligibility rules
      </button>
    </div>
  );
}
