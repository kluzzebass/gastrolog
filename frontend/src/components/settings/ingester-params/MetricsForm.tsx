import { FormField, TextInput } from "../FormField";
import type { SubFormProps } from "./types";

export function MetricsForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  return (
    <div className="grid grid-cols-2 gap-3">
      <FormField
        label="System Interval"
        description="How often to emit CPU/memory metrics"
        dark={dark}
      >
        <TextInput
          value={params["interval"] ?? ""}
          onChange={(v) => onChange({ ...params, interval: v })}
          placeholder={d["interval"] ?? ""}
          dark={dark}
          mono
          examples={["10s", "30s", "1m"]}
        />
      </FormField>
      <FormField
        label="Vault Interval"
        description="How often to emit vault stats metrics"
        dark={dark}
      >
        <TextInput
          value={params["vault_interval"] ?? ""}
          onChange={(v) => onChange({ ...params, vault_interval: v })}
          placeholder={d["vault_interval"] ?? ""}
          dark={dark}
          mono
          examples={["5s", "10s", "30s"]}
        />
      </FormField>
    </div>
  );
}
