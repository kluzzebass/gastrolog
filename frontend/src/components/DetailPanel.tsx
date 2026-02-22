import type { ProtoRecord } from "../utils";
import { useThemeClass } from "../hooks/useThemeClass";
import {
  extractKVPairs,
  relativeTime,
  formatBytes,
  formatChunkId,
} from "../utils";
import { syntaxHighlight, type HighlightMode } from "../syntax";
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
  highlightMode = "full",
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
  highlightMode?: HighlightMode;
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
    { label: "Ingest", date: record.ingestTs ? record.ingestTs.toDate() : null },
    { label: "Source", date: record.sourceTs ? record.sourceTs.toDate() : null },
  ];

  const headerCls = `pt-4 pb-1.5 text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`;
  const keyCls = `py-1 pr-2 w-[1%] text-[0.8em] font-mono whitespace-nowrap align-top ${c("text-text-ghost", "text-light-text-ghost")}`;
  const borderCls = `border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`;

  const valCls = `py-1 text-[0.8em] font-mono align-top ${borderCls}`;

  function valueCell(value: string, onClick?: () => void) {
    const cls = onClick
      ? `cursor-pointer transition-colors ${c("text-text-muted hover:text-copper", "text-light-text-muted hover:text-copper")}`
      : c("text-text-normal", "text-light-text-normal");
    return (
      <td className={valCls}>
        {onClick ? (
          <button
            type="button"
            onClick={onClick}
            className={`break-all text-left ${cls}`}
          >
            {value}
          </button>
        ) : (
          <span className={`break-all ${cls}`}>
            {value}
          </span>
        )}
      </td>
    );
  }

  return (
    <div className="p-4">
      <table className="w-full border-collapse">
        <tbody>
          {/* — Timestamps — */}
          <tr>
            <td colSpan={2} className={headerCls}>Timestamps</td>
          </tr>
          {tsRows.map(({ label, date }) => (
            <tr key={label}>
              <td className={`${keyCls} ${borderCls}`}>{label}</td>
              <td className={`${valCls}`}>
                {date ? (
                  <>
                    <div className={`break-all ${c("text-text-normal", "text-light-text-normal")}`}>
                      {date.toISOString()}
                    </div>
                    <div className={`text-[0.7em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
                      {relativeTime(date)}
                    </div>
                  </>
                ) : (
                  <div className={c("text-text-ghost", "text-light-text-ghost")}>
                    {"\u2014"}
                  </div>
                )}
              </td>
            </tr>
          ))}

          {/* — Message — */}
          <tr>
            <td colSpan={2} className={headerCls}>
              <span className="flex items-center gap-2">
                {`Message (${formatBytes(rawBytes)})`}
                <CopyButton text={rawText} dark={dark} />
              </span>
            </td>
          </tr>
          <tr>
            <td colSpan={2} className={`pb-1 ${borderCls}`}>
              <pre
                className={`text-[0.85em] font-mono p-3 rounded border whitespace-pre-wrap wrap-break-word leading-relaxed ${c("border-ink-border-subtle bg-ink text-text-normal", "border-light-border-subtle bg-light-bg text-light-text-normal")}`}
              >
                {syntaxHighlight(displayText, highlightMode).map((span, i) => {
                  const style = span.color ? { color: span.color } : undefined;
                  return span.url ? (
                    <a
                      key={`msg-${i}`}
                      href={span.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={style}
                      className="underline decoration-current/30 hover:decoration-current/60"
                    >
                      {span.text}
                    </a>
                  ) : (
                    <span key={`msg-${i}`} style={style}>
                      {span.text}
                    </span>
                  );
                })}
              </pre>
            </td>
          </tr>

          {/* — Attributes — */}
          {Object.keys(record.attrs).length > 0 && (
            <>
              <tr>
                <td colSpan={2} className={headerCls}>Attributes</td>
              </tr>
              {Object.entries(record.attrs).map(([k, v]) => (
                <tr key={k}>
                  <td className={`${keyCls} ${borderCls}`}>{k}</td>
                  {valueCell(v, onFieldSelect ? () => onFieldSelect(k, v) : undefined)}
                </tr>
              ))}
            </>
          )}

          {/* — Extracted Fields — */}
          {kvPairs.length > 0 && (
            <>
              <tr>
                <td colSpan={2} className={headerCls}>Extracted Fields</td>
              </tr>
              {kvPairs.map(({ key, value }, i) => (
                <tr key={`${key}-${i}`}>
                  <td className={`${keyCls} ${borderCls}`}>{key}</td>
                  {valueCell(value, onFieldSelect ? () => onFieldSelect(key, value) : undefined)}
                </tr>
              ))}
            </>
          )}

          {/* — Reference — */}
          <tr>
            <td colSpan={2} className={headerCls}>Reference</td>
          </tr>
          <tr>
            <td className={`${keyCls} ${borderCls}`}>Store</td>
            {valueCell(
              record.ref?.storeId ?? "N/A",
              record.ref?.storeId ? () => onStoreSelect?.(record.ref!.storeId) : undefined,
            )}
          </tr>
          <tr>
            <td className={`${keyCls} ${borderCls}`}>Chunk</td>
            {valueCell(
              record.ref?.chunkId ? formatChunkId(record.ref.chunkId) : "N/A",
              record.ref?.chunkId ? () => onChunkSelect?.(record.ref!.chunkId) : undefined,
            )}
          </tr>
          <tr>
            <td className={`${keyCls} ${borderCls}`}>Position</td>
            {valueCell(
              record.ref?.pos?.toString() ?? "N/A",
              record.ref?.chunkId && record.ref?.pos != null
                ? () => onPosSelect?.(record.ref!.chunkId, record.ref!.pos.toString())
                : undefined,
            )}
          </tr>

          {/* — Context — */}
          <tr>
            <td colSpan={2} className={headerCls}>Context</td>
          </tr>
          <tr>
            <td colSpan={2} className="pb-1">
              <ContextSection
                dark={dark}
                record={record}
                contextBefore={contextBefore}
                contextAfter={contextAfter}
                contextLoading={contextLoading}
                contextReversed={contextReversed}
                onContextRecordSelect={onContextRecordSelect}
                highlightMode={highlightMode}
              />
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

function ContextList({
  records,
  prefix,
  dark,
  onSelect,
  highlightMode = "full",
}: Readonly<{
  records: ProtoRecord[] | undefined;
  prefix: string;
  dark: boolean;
  onSelect?: (rec: ProtoRecord) => void;
  highlightMode?: HighlightMode;
}>) {
  return records?.map((rec, i) => (
    <ContextRecord
      key={`${prefix}-${i}`}
      record={rec}
      isAnchor={false}
      dark={dark}
      onSelect={onSelect ? () => onSelect(rec) : undefined}
      highlightMode={highlightMode}
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
  highlightMode = "full",
}: Readonly<{
  dark: boolean;
  record: ProtoRecord;
  contextBefore?: ProtoRecord[];
  contextAfter?: ProtoRecord[];
  contextLoading?: boolean;
  contextReversed?: boolean;
  onContextRecordSelect?: (record: ProtoRecord) => void;
  highlightMode?: HighlightMode;
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
      highlightMode={highlightMode}
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
      <ContextList records={before} prefix={beforePrefix} dark={dark} onSelect={onContextRecordSelect} highlightMode={highlightMode} />
      {anchor}
      <ContextList records={after} prefix={afterPrefix} dark={dark} onSelect={onContextRecordSelect} highlightMode={highlightMode} />
    </div>
  );
}
