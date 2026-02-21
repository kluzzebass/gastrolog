import { FormField, TextInput } from "../settings/FormField";
import { IngesterParamsForm } from "../settings/IngesterParamsForm";
import { useThemeClass } from "../../hooks/useThemeClass";

export interface IngesterData {
  name: string;
  type: string;
  params: Record<string, string>;
}

interface IngesterStepProps {
  dark: boolean;
  data: IngesterData;
  onChange: (data: IngesterData) => void;
}

const INGESTER_TYPES = [
  { id: "syslog", label: "Syslog", description: "RFC 3164/5424 UDP + TCP" },
  { id: "http", label: "HTTP (Loki)", description: "Loki-compatible push API" },
  { id: "otlp", label: "OTLP", description: "OpenTelemetry logs (HTTP + gRPC)" },
  { id: "fluentfwd", label: "Fluent Forward", description: "Fluentd / Fluent Bit protocol" },
  { id: "kafka", label: "Kafka", description: "Kafka topic consumer" },
  { id: "docker", label: "Docker", description: "Container log streaming" },
  { id: "tail", label: "Tail", description: "File tailing with glob patterns" },
  { id: "relp", label: "RELP", description: "Reliable Event Logging Protocol" },
  { id: "chatterbox", label: "Chatterbox", description: "Test data generator" },
] as const;

export function IngesterStep({ dark, data, onChange }: Readonly<IngesterStepProps>) {
  const c = useThemeClass(dark);

  const selectType = (type: string) => {
    // Keep user-typed name; clear if it still matches the previous auto-name.
    const isAutoName = !data.name || data.name === data.type;
    onChange({
      name: isAutoName ? "" : data.name,
      type,
      params: {},
    });
  };

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-1">
        <h2
          className={`text-lg font-display font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Configure Ingester
        </h2>
        <p
          className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          An ingester receives log data and feeds it into your store. You can add more ingesters later in Settings.
        </p>
      </div>

      {/* Type selection cards */}
      <div className="grid grid-cols-3 gap-2">
        {INGESTER_TYPES.map((t) => (
          <button
            key={t.id}
            type="button"
            onClick={() => selectType(t.id)}
            className={`flex flex-col gap-0.5 px-3 py-2.5 rounded border text-left transition-colors cursor-pointer ${
              data.type === t.id
                ? c(
                    "border-copper bg-copper/10 text-text-bright",
                    "border-copper bg-copper/10 text-light-text-bright",
                  )
                : c(
                    "border-ink-border bg-ink-surface text-text-muted hover:border-ink-border-subtle hover:bg-ink-hover",
                    "border-light-border bg-light-surface text-light-text-muted hover:border-light-border-subtle hover:bg-light-hover",
                  )
            }`}
          >
            <span className="text-[0.85em] font-medium">{t.label}</span>
            <span
              className={`text-[0.7em] ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              {t.description}
            </span>
          </button>
        ))}
      </div>

      {data.type && (
        <>
          <FormField label="Ingester Name" dark={dark}>
            <TextInput
              value={data.name}
              onChange={(v) => onChange({ ...data, name: v })}
              placeholder={data.type}
              dark={dark}
            />
          </FormField>

          <IngesterParamsForm
            ingesterType={data.type}
            params={data.params}
            onChange={(params) => onChange({ ...data, params })}
            dark={dark}
          />
        </>
      )}
    </div>
  );
}
