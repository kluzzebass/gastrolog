import { FormField, TextInput } from "../FormField";
import { TestConnectionButton } from "./TestConnectionButton";
import type { SubFormProps } from "./types";

export function OtlpForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  return (
    <div className="flex flex-col gap-3">
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="HTTP Address"
          description="OTLP/HTTP listen address (POST /v1/logs)"
          dark={dark}
        >
          <TextInput
            value={params["http_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, http_addr: v })}
            placeholder={d["http_addr"] ?? ""}
            dark={dark}
            mono
            examples={[":4318"]}
          />
        </FormField>
        <FormField
          label="gRPC Address"
          description="OTLP/gRPC listen address"
          dark={dark}
        >
          <TextInput
            value={params["grpc_addr"] ?? ""}
            onChange={(v) => onChange({ ...params, grpc_addr: v })}
            placeholder={d["grpc_addr"] ?? ""}
            dark={dark}
            mono
            examples={[":4317"]}
          />
        </FormField>
      </div>
      <TestConnectionButton type="otlp" params={params} dark={dark} />
    </div>
  );
}
