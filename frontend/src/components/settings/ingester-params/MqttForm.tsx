import { FormField, TextInput, SelectInput } from "../FormField";
import { Checkbox } from "../Checkbox";
import { TestConnectionButton } from "./TestConnectionButton";
import type { SubFormProps } from "./types";

export function MqttForm({
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
        label="Broker"
        description="MQTT broker URL (required)"
        dark={dark}
      >
        <TextInput
          value={params["broker"] ?? ""}
          onChange={(v) => set("broker", v)}
          placeholder=""
          dark={dark}
          mono
          examples={["mqtt://localhost:1883", "ssl://broker:8883", "ws://broker:8080/mqtt"]}
        />
      </FormField>
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Topics"
          description="Comma-separated topic patterns (supports + and # wildcards)"
          dark={dark}
        >
          <TextInput
            value={params["topics"] ?? ""}
            onChange={(v) => set("topics", v)}
            placeholder=""
            dark={dark}
            mono
            examples={["sensors/#", "home/+/temperature", "logs,events"]}
          />
        </FormField>
        <FormField
          label="Client ID"
          description="MQTT client identifier. If left empty, auto-generated as gastrolog-<last 8 chars of ingester ID>"
          dark={dark}
        >
          <TextInput
            value={params["client_id"] ?? ""}
            onChange={(v) => set("client_id", v)}
            placeholder={d["client_id"] ?? "gastrolog-<auto>"}
            dark={dark}
            mono
          />
        </FormField>
      </div>
      <FormField
        label="Protocol Version"
        description="MQTT protocol version"
        dark={dark}
      >
        <SelectInput
          value={params["version"] ?? d["version"] ?? "3"}
          onChange={(v) => set("version", v)}
          options={[
            { value: "3", label: "v3.1.1" },
            { value: "5", label: "v5" },
          ]}
          dark={dark}
        />
      </FormField>
      <div className="flex gap-6">
        <Checkbox
          checked={params["tls"] === "true"}
          onChange={(v) => set("tls", v ? "true" : "false")}
          label="Enable TLS"
          dark={dark}
        />
        <Checkbox
          checked={params["clean_session"] !== "false"}
          onChange={(v) => set("clean_session", v ? "true" : "false")}
          label="Clean session"
          helpTopicId="ingester-mqtt"
          dark={dark}
        />
      </div>
      <div className="grid grid-cols-2 gap-3">
        <FormField label="Username" dark={dark}>
          <TextInput
            value={params["username"] ?? ""}
            onChange={(v) => set("username", v)}
            dark={dark}
            mono
          />
        </FormField>
        <FormField label="Password" dark={dark}>
          <TextInput
            value={params["password"] ?? ""}
            onChange={(v) => set("password", v)}
            dark={dark}
            mono
          />
        </FormField>
      </div>
      <TestConnectionButton type="mqtt" params={params} dark={dark} />
    </div>
  );
}
