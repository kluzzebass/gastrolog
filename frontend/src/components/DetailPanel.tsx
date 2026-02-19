import type { ProtoRecord } from "../utils";
import { useThemeClass } from "../hooks/useThemeClass";
import {
  extractKVPairs,
  relativeTime,
  formatBytes,
  formatChunkId,
} from "../utils";
import { syntaxHighlight } from "../syntax";
import { CopyButton } from "./CopyButton";
import { ContextRecord } from "./ContextRecord";


export function DetailPanelContent({
  record,
  dark,
  onFieldSelect,
  onChunkSelect,
  onStoreSelect,
  onPosSelect,
  contextBefore,
  contextAfter,
  contextLoading,
  contextReversed,
  onContextRecordSelect,
}: Readonly<{
  record: ProtoRecord;
  dark: boolean;
  onFieldSelect?: (key: string, value: string) => void;
  onChunkSelect?: (chunkId: string) => void;
  onStoreSelect?: (storeId: string) => void;
  onPosSelect?: (chunkId: string, pos: string) => void;
  contextBefore?: ProtoRecord[];
  contextAfter?: ProtoRecord[];
  contextLoading?: boolean;
  contextReversed?: boolean;
  onContextRecordSelect?: (record: ProtoRecord) => void;
}>) {
  const c = useThemeClass(dark);
  const rawText = new TextDecoder().decode(record.raw);
  const rawBytes = record.raw.length;

  // Pretty-print JSON for the detail view.
  let displayText = rawText;
  if (rawText.trimStart().startsWith("{")) {
    try {
      displayText = JSON.stringify(JSON.parse(rawText), null, 2);
    } catch {
      // Not valid JSON — use raw text as-is.
    }
  }
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
      <DetailSection
        label={`Message (${formatBytes(rawBytes)})`}
        dark={dark}
        action={<CopyButton text={rawText} dark={dark} />}
      >
        <pre
          className={`text-[0.85em] font-mono p-3 rounded border whitespace-pre-wrap wrap-break-word leading-relaxed ${c("border-ink-border-subtle bg-ink text-text-normal", "border-light-border-subtle bg-light-bg text-light-text-normal")}`}
        >
          {syntaxHighlight(displayText).map((span, i) => {
            const style = span.color ? { color: span.color } : undefined;
            return span.url ? (
              <a
                key={i} // NOSONAR: stable derived list, no natural key
                href={span.url}
                target="_blank"
                rel="noopener noreferrer"
                style={style}
                className="underline decoration-current/30 hover:decoration-current/60"
              >
                {span.text}
              </a>
            ) : (
              <span key={i} style={style}> {/* NOSONAR: stable derived list, no natural key */}
                {span.text}
              </span>
            );
          })}
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

      {/* Context */}
      <DetailSection label="Context" dark={dark}>
        <ContextSection
          dark={dark}
          record={record}
          contextBefore={contextBefore}
          contextAfter={contextAfter}
          contextLoading={contextLoading}
          contextReversed={contextReversed}
          onContextRecordSelect={onContextRecordSelect}
        />
      </DetailSection>
    </div>
  );
}

function ContextList({
  records,
  prefix,
  dark,
  onSelect,
}: Readonly<{
  records: ProtoRecord[] | undefined;
  prefix: string;
  dark: boolean;
  onSelect?: (rec: ProtoRecord) => void;
}>) {
  return records?.map((rec, i) => (
    <ContextRecord
      key={`${prefix}-${i}`}
      record={rec}
      isAnchor={false}
      dark={dark}
      onSelect={onSelect ? () => onSelect(rec) : undefined}
    />
  ));
}

function ContextSection({
  dark,
  record,
  contextBefore,
  contextAfter,
  contextLoading,
  contextReversed,
  onContextRecordSelect,
}: Readonly<{
  dark: boolean;
  record: ProtoRecord;
  contextBefore?: ProtoRecord[];
  contextAfter?: ProtoRecord[];
  contextLoading?: boolean;
  contextReversed?: boolean;
  onContextRecordSelect?: (record: ProtoRecord) => void;
}>) {
  const c = useThemeClass(dark);

  if (contextLoading) {
    return (
      <div className={`text-[0.8em] font-mono py-2 ${c("text-text-ghost", "text-light-text-ghost")}`}>
        Loading context...
      </div>
    );
  }

  const hasContext = (contextBefore?.length ?? 0) > 0 || (contextAfter?.length ?? 0) > 0;
  if (!hasContext) {
    return (
      <div className={`text-[0.8em] font-mono py-2 ${c("text-text-ghost", "text-light-text-ghost")}`}>
        No surrounding records
      </div>
    );
  }

  const anchor = (
    <ContextRecord
      key="anchor"
      record={record}
      isAnchor={true}
      dark={dark}
      onSelect={onContextRecordSelect ? () => onContextRecordSelect(record) : undefined}
    />
  );

  // Reversed (newest first): after (reversed), anchor, before (reversed)
  // Forward (oldest first): before, anchor, after
  const before = contextReversed ? contextAfter?.slice().reverse() : contextBefore;
  const after = contextReversed ? contextBefore?.slice().reverse() : contextAfter;
  const beforePrefix = contextReversed ? "after" : "before";
  const afterPrefix = contextReversed ? "before" : "after";

  return (
    <div className={`rounded overflow-hidden border ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-bg")}`}>
      <ContextList records={before} prefix={beforePrefix} dark={dark} onSelect={onContextRecordSelect} />
      {anchor}
      <ContextList records={after} prefix={afterPrefix} dark={dark} onSelect={onContextRecordSelect} />
    </div>
  );
}

function DetailSection({
  label,
  dark,
  children,
  action,
}: Readonly<{
  label: string;
  dark: boolean;
  children: React.ReactNode;
  action?: React.ReactNode;
}>) {
  const c = useThemeClass(dark);
  return (
    <div>
      <div className="flex items-center gap-2 mb-1.5">
        <h4
          className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          {label}
        </h4>
        {action}
      </div>
      {children}
    </div>
  );
}

function DetailRow({
  label,
  value,
  dark,
  onClick,
}: Readonly<{
  label: string;
  value: string;
  dark: boolean;
  onClick?: () => void;
}>) {
  const c = useThemeClass(dark);
  const valueClass = onClick
    ? `cursor-pointer transition-colors ${c("text-text-muted hover:text-copper", "text-light-text-muted hover:text-copper")}`
    : c("text-text-normal", "text-light-text-normal");

  return (
    <div
      className={`flex py-1 border-b last:border-b-0 ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    >
      <dt
        className={`w-24 shrink-0 text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        {label}
      </dt>
      <dd className="flex-1 min-w-0">
        {onClick ? (
          <button
            type="button"
            onClick={onClick}
            className={`text-[0.85em] font-mono break-all text-left w-full ${valueClass}`}
          >
            {value}
          </button>
        ) : (
          <span className={`text-[0.85em] font-mono break-all ${valueClass}`}>
            {value}
          </span>
        )}
      </dd>
    </div>
  );
}
