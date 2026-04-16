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
import { ScatterboxForm } from "./ScatterboxForm";
import { MetricsForm } from "./MetricsForm";
import { SelfForm } from "./SelfForm";
import { SyslogForm } from "./SyslogForm";
import type { IngesterParamsFormProps } from "./types";

export type { IngesterParamsFormProps } from "./types";
export { isIngesterParamsValid, listenAddrConflict } from "./validation";

const FORM_MAP: Record<
  string,
  React.ComponentType<{
    params: Record<string, string>;
    onChange: (params: Record<string, string>) => void;
    dark: boolean;
    defaults: Record<string, string>;
    ingesterId?: string;
    ingesterNodeId?: string;
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
  scatterbox: ScatterboxForm,
  metrics: MetricsForm,
  self: SelfForm,
  syslog: SyslogForm,
};

export function IngesterParamsForm({
  ingesterType,
  params,
  onChange,
  dark,
  ingesterId,
  ingesterNodeId,
}: Readonly<IngesterParamsFormProps>) {
  const { data: ingesterMeta } = useIngesterDefaults();
  const defaults = ingesterMeta?.defaults[ingesterType] ?? {};

  const Form = FORM_MAP[ingesterType];
  if (!Form) return null;

  return (
    <Form params={params} onChange={onChange} dark={dark} defaults={defaults} ingesterId={ingesterId} ingesterNodeId={ingesterNodeId} />
  );
}
