import { FormField, SelectInput } from "../FormField";
import type { SubFormProps } from "./types";

const LEVEL_OPTIONS = [
  { value: "debug", label: "Debug" },
  { value: "info", label: "Info" },
  { value: "warn", label: "Warn" },
  { value: "error", label: "Error" },
];

export function SelfForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  return (
    <FormField
      label="Minimum Level"
      description="Only capture log records at or above this severity"
      dark={dark}
    >
      <SelectInput
        value={params["min_level"] || d["min_level"] || "warn"}
        onChange={(v) => onChange({ ...params, min_level: v })}
        options={LEVEL_OPTIONS}
        dark={dark}
      />
    </FormField>
  );
}
