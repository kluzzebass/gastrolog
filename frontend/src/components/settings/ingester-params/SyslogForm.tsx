import { FormField, TextInput } from "../FormField";
import { TestConnectionButton } from "./TestConnectionButton";
import type { SubFormProps } from "./types";

export function SyslogForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  return (
    <div className="flex flex-col gap-3">
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="UDP Address"
          description="UDP listen address"
          dark={dark}
        >
          <TextInput
            value={params["udp_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, udp_addr: v })}
            placeholder={d["udp_addr"] ?? ""}
            dark={dark}
            mono
            examples={[":514"]}
          />
        </FormField>
        <FormField
          label="TCP Address"
          description="TCP listen address (leave empty to disable)"
          dark={dark}
        >
          <TextInput
            value={params["tcp_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, tcp_addr: v })}
            placeholder=""
            dark={dark}
            mono
            examples={[":514"]}
          />
        </FormField>
      </div>
      <TestConnectionButton type="syslog" params={params} dark={dark} />
    </div>
  );
}
