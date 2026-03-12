import { FormField, TextInput } from "../FormField";
import type { SubFormProps } from "./types";

export function FluentfwdForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  return (
    <div className="flex flex-col gap-3">
      <FormField
        label="Listen Address"
        description="TCP address for Fluent Forward protocol"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => onChange({ ...params, addr: v })}
          placeholder={d["addr"] ?? ""}
          dark={dark}
          mono
          examples={[":24224"]}
        />
      </FormField>
    </div>
  );
}
