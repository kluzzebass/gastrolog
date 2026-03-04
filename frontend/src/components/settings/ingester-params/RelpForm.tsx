import { FormField, TextInput } from "../FormField";
import { TestConnectionButton } from "./TestConnectionButton";
import type { SubFormProps } from "./types";

export function RelpForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  return (
    <div className="flex flex-col gap-3">
      <FormField
        label="Listen Address"
        description="TCP address for RELP"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => onChange({ ...params, addr: v })}
          placeholder={d["addr"] ?? ""}
          dark={dark}
          mono
          examples={[":2514"]}
        />
      </FormField>
      <TestConnectionButton type="relp" params={params} dark={dark} />
    </div>
  );
}
