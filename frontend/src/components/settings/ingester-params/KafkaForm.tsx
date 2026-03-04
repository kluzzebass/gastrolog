import { FormField, TextInput, SelectInput } from "../FormField";
import { Checkbox } from "../Checkbox";
import { TestConnectionButton } from "./TestConnectionButton";
import type { SubFormProps } from "./types";

export function KafkaForm({
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
        label="Brokers"
        description="Comma-separated list of Kafka broker addresses (required)"
        dark={dark}
      >
        <TextInput
          value={params["brokers"] ?? ""}
          onChange={(v) => set("brokers", v)}
          placeholder=""
          dark={dark}
          mono
          examples={["localhost:9092", "broker1:9092,broker2:9092"]}
        />
      </FormField>
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Topic"
          description="Kafka topic to consume (required)"
          dark={dark}
        >
          <TextInput
            value={params["topic"] ?? ""}
            onChange={(v) => set("topic", v)}
            placeholder=""
            dark={dark}
            mono
            examples={["logs", "app-logs"]}
          />
        </FormField>
        <FormField
          label="Consumer Group"
          description="Kafka consumer group ID"
          dark={dark}
        >
          <TextInput
            value={params["group"] ?? ""}
            onChange={(v) => set("group", v)}
            placeholder={d["group"] ?? ""}
            dark={dark}
            mono
            examples={["gastrolog"]}
          />
        </FormField>
      </div>
      <Checkbox
        checked={params["tls"] === "true"}
        onChange={(v) => set("tls", v ? "true" : "false")}
        label="Enable TLS"
        dark={dark}
      />
      <FormField
        label="SASL Mechanism"
        description="Authentication mechanism (leave empty to disable)"
        dark={dark}
      >
        <SelectInput
          value={params["sasl_mechanism"] ?? ""}
          onChange={(v) => set("sasl_mechanism", v)}
          options={[
            { value: "", label: "(none)" },
            { value: "plain", label: "PLAIN" },
            { value: "scram-sha-256", label: "SCRAM-SHA-256" },
            { value: "scram-sha-512", label: "SCRAM-SHA-512" },
          ]}
          dark={dark}
        />
      </FormField>
      {params["sasl_mechanism"] && (
        <div className="grid grid-cols-2 gap-3">
          <FormField label="SASL User" dark={dark}>
            <TextInput
              value={params["sasl_user"] ?? ""}
              onChange={(v) => set("sasl_user", v)}
              dark={dark}
              mono
            />
          </FormField>
          <FormField label="SASL Password" dark={dark}>
            <TextInput
              value={params["sasl_password"] ?? ""}
              onChange={(v) => set("sasl_password", v)}
              dark={dark}
              mono
            />
          </FormField>
        </div>
      )}
      <TestConnectionButton type="kafka" params={params} dark={dark} />
    </div>
  );
}
