import { useIngesterDefaults } from "../../../api/hooks/useIngesterDefaults";
import { ChatterboxForm } from "./ChatterboxForm";
import { TailForm } from "./TailForm";
import { DockerForm } from "./DockerForm";
import { OtlpForm } from "./OtlpForm";
import { FluentfwdForm } from "./FluentfwdForm";
import { KafkaForm } from "./KafkaForm";
import { MqttForm } from "./MqttForm";
import { HttpForm } from "./HttpForm";
import { RelpForm } from "./RelpForm";
import { MetricsForm } from "./MetricsForm";
import { SyslogForm } from "./SyslogForm";
import type { IngesterParamsFormProps } from "./types";

export type { IngesterParamsFormProps } from "./types";
export { isIngesterParamsValid } from "./validation";

const FORM_MAP: Record<
  string,
  React.ComponentType<{
    params: Record<string, string>;
    onChange: (params: Record<string, string>) => void;
    dark: boolean;
    defaults: Record<string, string>;
  }>
> = {
  chatterbox: ChatterboxForm,
  tail: TailForm,
  docker: DockerForm,
  otlp: OtlpForm,
  fluentfwd: FluentfwdForm,
  kafka: KafkaForm,
  mqtt: MqttForm,
  http: HttpForm,
  relp: RelpForm,
  metrics: MetricsForm,
  syslog: SyslogForm,
};

export function IngesterParamsForm({
  ingesterType,
  params,
  onChange,
  dark,
}: Readonly<IngesterParamsFormProps>) {
  const { data: allDefaults } = useIngesterDefaults();
  const defaults = allDefaults?.[ingesterType] ?? {};

  const Form = FORM_MAP[ingesterType];
  if (!Form) return null;

  return (
    <Form params={params} onChange={onChange} dark={dark} defaults={defaults} />
  );
}
