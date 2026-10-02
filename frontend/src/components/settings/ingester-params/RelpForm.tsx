import { FormField, TextInput } from "../FormField";
import { TlsListenerFields } from "./TlsFields";
import type { SubFormProps } from "./types";

export function RelpForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });

  return (
    <div className="flex flex-col gap-3">
      <FormField
        label="Listen Address"
        description="TCP address for RELP"
        dark={dark}
      >
        <TextInput
          value={params["addr"] ?? ""}
          onChange={(v) => set("addr", v)}
          placeholder={d["addr"] ?? ""}
          dark={dark}
          mono
          examples={[":2514"]}
        />
      </FormField>
      <TlsListenerFields params={params} onChange={onChange} dark={dark} />
    </div>
  );
}
