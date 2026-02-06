import type { ProtoRecord } from "../utils";
import { extractKVPairs, relativeTime, formatBytes, formatChunkId } from "../utils";

export function DetailPanelContent({
  record,
  dark,
  onFieldSelect,
  onChunkSelect,
  onStoreSelect,
  onPosSelect,
}: {
  record: ProtoRecord;
  dark: boolean;
  onFieldSelect?: (key: string, value: string) => void;
  onChunkSelect?: (chunkId: string) => void;
  onStoreSelect?: (storeId: string) => void;
  onPosSelect?: (chunkId: string, pos: string) => void;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  const rawText = new TextDecoder().decode(record.raw);
  const rawBytes = record.raw.length;
  const kvPairs = extractKVPairs(rawText);

  const tsRows: { label: string; date: Date | null }[] = [
    { label: "Write", date: record.writeTs ? record.writeTs.toDate() : null },
    {
      label: "Ingest",
      date: record.ingestTs ? record.ingestTs.toDate() : null,
    },
    {
      label: "Source",
      date: record.sourceTs ? record.sourceTs.toDate() : null,
    },
  ];

  return (
    <div className="p-4 space-y-4">
      {/* Timestamps */}
      <DetailSection label="Timestamps" dark={dark}>
        <div className="space-y-1.5">
          {tsRows.map(({ label, date }) => (
            <div
              key={label}
              className={`flex py-1 border-b last:border-b-0 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
            >
              <dt
                className={`w-16 shrink-0 text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
              >
                {label}
              </dt>
              <dd className="flex-1 min-w-0">
                {date ? (
                  <>
                    <div
                      className={`text-[0.85em] font-mono break-all ${c("text-text-normal", "text-light-text-normal")}`}
                    >
                      {date.toISOString()}
                    </div>
                    <div
                      className={`text-[0.75em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      {relativeTime(date)}
                    </div>
                  </>
                ) : (
                  <>
                    <div
                      className={`text-[0.85em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
                    >
                      {"\u2014"}
                    </div>
                    <div className="text-[0.75em] font-mono">&nbsp;</div>
                  </>
                )}
              </dd>
            </div>
          ))}
        </div>
      </DetailSection>

      {/* Message */}
      <DetailSection label={`Message (${formatBytes(rawBytes)})`} dark={dark}>
        <pre
          className={`text-[0.85em] font-mono p-3 rounded whitespace-pre-wrap break-words leading-relaxed ${c("bg-ink text-text-normal", "bg-light-bg text-light-text-normal")}`}
        >
          {rawText}
        </pre>
      </DetailSection>

      {/* Attributes */}
      {Object.keys(record.attrs).length > 0 && (
        <DetailSection label="Attributes" dark={dark}>
          <div className="space-y-0">
            {Object.entries(record.attrs).map(([k, v]) => (
              <DetailRow
                key={k}
                label={k}
                value={v}
                dark={dark}
                onClick={onFieldSelect ? () => onFieldSelect(k, v) : undefined}
              />
            ))}
          </div>
        </DetailSection>
      )}

      {/* Extracted Fields */}
      {kvPairs.length > 0 && (
        <DetailSection label="Extracted Fields" dark={dark}>
          <div className="space-y-0">
            {kvPairs.map(({ key, value }, i) => (
              <DetailRow
                key={`${key}-${i}`}
                label={key}
                value={value}
                dark={dark}
                onClick={
                  onFieldSelect ? () => onFieldSelect(key, value) : undefined
                }
              />
            ))}
          </div>
        </DetailSection>
      )}

      {/* Reference */}
      <DetailSection label="Reference" dark={dark}>
        <div className="space-y-0">
          <DetailRow
            label="Store"
            value={record.ref?.storeId ?? "N/A"}
            dark={dark}
            onClick={
              record.ref?.storeId
                ? () => onStoreSelect?.(record.ref!.storeId)
                : undefined
            }
          />
          <DetailRow
            label="Chunk"
            value={
              record.ref?.chunkId ? formatChunkId(record.ref.chunkId) : "N/A"
            }
            dark={dark}
            onClick={
              record.ref?.chunkId
                ? () => onChunkSelect?.(record.ref!.chunkId)
                : undefined
            }
          />
          <DetailRow
            label="Position"
            value={record.ref?.pos?.toString() ?? "N/A"}
            dark={dark}
            onClick={
              record.ref?.chunkId && record.ref?.pos != null
                ? () =>
                    onPosSelect?.(
                      record.ref!.chunkId,
                      record.ref!.pos.toString(),
                    )
                : undefined
            }
          />
        </div>
      </DetailSection>
    </div>
  );
}

export function DetailSection({
  label,
  dark,
  children,
}: {
  label: string;
  dark: boolean;
  children: React.ReactNode;
}) {
  return (
    <div>
      <h4
        className={`text-[0.7em] font-medium uppercase tracking-[0.15em] mb-1.5 ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {label}
      </h4>
      {children}
    </div>
  );
}

export function DetailRow({
  label,
  value,
  dark,
  onClick,
}: {
  label: string;
  value: string;
  dark: boolean;
  onClick?: () => void;
}) {
  return (
    <div
      className={`flex py-1 border-b last:border-b-0 ${dark ? "border-ink-border-subtle" : "border-light-border-subtle"}`}
    >
      <dt
        className={`w-24 shrink-0 text-[0.8em] ${dark ? "text-text-ghost" : "text-light-text-ghost"}`}
      >
        {label}
      </dt>
      <dd
        className={`flex-1 text-[0.85em] font-mono break-all ${
          onClick
            ? `cursor-pointer transition-colors ${dark ? "text-text-muted hover:text-copper" : "text-light-text-muted hover:text-copper"}`
            : dark
              ? "text-text-normal"
              : "text-light-text-normal"
        }`}
        onClick={onClick}
      >
        {value}
      </dd>
    </div>
  );
}
