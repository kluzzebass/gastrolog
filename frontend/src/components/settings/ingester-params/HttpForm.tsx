import { FormField, TextInput } from "../FormField";
import { TestConnectionButton } from "./TestConnectionButton";
import type { SubFormProps } from "./types";

export function HttpForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  return (
    <div className="flex flex-col gap-3">
      <FormField
        label="Listen Address"
        description="TCP address for HTTP/Loki Push API"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => onChange({ ...params, addr: v })}
          placeholder={d["addr"] ?? ""}
          dark={dark}
          mono
          examples={[":3100"]}
        />
      </FormField>
      <TestConnectionButton type="http" params={params} dark={dark} />
    </div>
  );
}
