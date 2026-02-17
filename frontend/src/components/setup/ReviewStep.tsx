import { useThemeClass } from "../../hooks/useThemeClass";
import type { StoreData } from "./StoreStep";
import type { RotationData, RetentionData } from "./PoliciesStep";
import type { IngesterData } from "./IngesterStep";

interface ReviewStepProps {
  dark: boolean;
  store: StoreData;
  rotation: RotationData;
  retention: RetentionData;
  ingester: IngesterData;
}

function Section({
  title,
  dark,
  children,
}: {
  title: string;
  dark: boolean;
  children: React.ReactNode;
}) {
  const c = useThemeClass(dark);
  return (
    <div className="flex flex-col gap-1.5">
      <h3
        className={`text-[0.8em] font-medium uppercase tracking-[0.1em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        {title}
      </h3>
      <div
        className={`rounded border px-3 py-2 text-[0.85em] flex flex-col gap-1 ${c(
          "border-ink-border bg-ink-surface",
          "border-light-border bg-light-surface",
        )}`}
      >
        {children}
      </div>
    </div>
  );
}

function SkippedSection({ title, dark }: { title: string; dark: boolean }) {
  const c = useThemeClass(dark);
  return (
    <div className="flex flex-col gap-1.5">
      <h3
        className={`text-[0.8em] font-medium uppercase tracking-[0.1em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        {title}
      </h3>
      <div
        className={`text-[0.85em] italic ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        None
      </div>
    </div>
  );
}

function Row({
  label,
  value,
  dark,
}: {
  label: string;
  value: string;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  if (!value) return null;
  return (
    <div className="flex justify-between gap-4">
      <span className={c("text-text-muted", "text-light-text-muted")}>
        {label}
      </span>
      <span
        className={`font-mono ${c("text-text-bright", "text-light-text-bright")}`}
      >
        {value}
      </span>
    </div>
  );
}

export function ReviewStep({
  dark,
  store,
  rotation,
  retention,
  ingester,
}: ReviewStepProps) {
  const c = useThemeClass(dark);
  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-1">
        <h2
          className={`text-lg font-display font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Review & Create
        </h2>
        <p
          className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Review your configuration before creating everything.
        </p>
      </div>

      <Section title="Store" dark={dark}>
        <Row label="Name" value={store.name || "default"} dark={dark} />
        <Row label="Type" value={store.type} dark={dark} />
        {store.type === "file" && (
          <Row label="Directory" value={store.dir} dark={dark} />
        )}
      </Section>

      {(rotation.maxAge || rotation.maxBytes || rotation.maxRecords || rotation.cron) ? (
        <Section title="Rotation Policy" dark={dark}>
          <Row label="Name" value={rotation.name || "default"} dark={dark} />
          {rotation.maxAge && <Row label="Max Age" value={rotation.maxAge} dark={dark} />}
          {rotation.maxBytes && <Row label="Max Bytes" value={rotation.maxBytes} dark={dark} />}
          {rotation.maxRecords && <Row label="Max Records" value={rotation.maxRecords} dark={dark} />}
          {rotation.cron && <Row label="Cron" value={rotation.cron} dark={dark} />}
        </Section>
      ) : (
        <SkippedSection title="Rotation Policy" dark={dark} />
      )}

      {(retention.maxChunks || retention.maxAge || retention.maxBytes) ? (
        <Section title="Retention Policy" dark={dark}>
          <Row label="Name" value={retention.name || "default"} dark={dark} />
          {retention.maxChunks && <Row label="Max Chunks" value={retention.maxChunks} dark={dark} />}
          {retention.maxAge && <Row label="Max Age" value={retention.maxAge} dark={dark} />}
          {retention.maxBytes && <Row label="Max Bytes" value={retention.maxBytes} dark={dark} />}
        </Section>
      ) : (
        <SkippedSection title="Retention Policy" dark={dark} />
      )}

      <Section title="Ingester" dark={dark}>
        <Row label="Name" value={ingester.name || ingester.type} dark={dark} />
        <Row label="Type" value={ingester.type} dark={dark} />
        {Object.entries(ingester.params)
          .filter(([, v]) => v)
          .map(([k, v]) => (
            <Row key={k} label={k} value={v} dark={dark} />
          ))}
      </Section>
    </div>
  );
}
