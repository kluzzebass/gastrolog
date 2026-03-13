import { useState } from "react";
import type { ProtoRecord } from "../utils";
import { useThemeClass } from "../hooks/useThemeClass";
import {
  extractKVPairs,
  formatBytes,
  formatChunkId,
} from "../utils";
import { protoToInstant, instantToISO, relativeTime } from "../utils/temporal";
import { syntaxHighlight, type HighlightMode } from "../syntax";
import { CopyButton } from "./CopyButton";
import { ContextRecord } from "./ContextRecord";
import { useDetailPanel } from "../hooks/useDetailPanel";
import { useConfig } from "../api/hooks";

const MAX_DISPLAY_LINES = 100;


/** Format a 16-byte Uint8Array as a UUID string. */
function formatUUID(bytes: Uint8Array): string {
  if (bytes.length !== 16) return "";
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}


interface DetailStyles {
  headerCls: string;
  keyCls: string;
  borderCls: string;
  valCls: string;
  c: ReturnType<typeof useThemeClass>;
}

function buildStyles(c: ReturnType<typeof useThemeClass>): DetailStyles {
  const headerCls = `pt-4 pb-1.5 text-left text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`;
  const keyCls = `py-1 pr-2 w-0 text-[0.8em] font-mono whitespace-nowrap align-top ${c("text-text-ghost", "text-light-text-ghost")}`;
  const borderCls = `border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`;
  const valCls = `py-1 text-[0.8em] font-mono align-top ${borderCls}`;
  return { headerCls, keyCls, borderCls, valCls, c };
}

function ValueCell({
  value,
  onClick,
  styles,
}: Readonly<{
  value: string;
  onClick?: () => void;
  styles: DetailStyles;
}>) {
  const cls = onClick
    ? `cursor-pointer transition-colors ${styles.c("text-text-muted hover:text-copper", "text-light-text-muted hover:text-copper")}`
    : styles.c("text-text-normal", "text-light-text-normal");
  return (
    <td className={styles.valCls}>
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

function MessageSection({
  rawText,
  rawBytes,
  visibleText,
  isLarge,
  displayLineCount,
  collapsed,
  setCollapsed,
  dark,
  highlightMode,
  onSpanClick,
  styles,
}: Readonly<{
  rawText: string;
  rawBytes: number;
  visibleText: string;
  isLarge: boolean;
  displayLineCount: number;
  collapsed: boolean;
  setCollapsed: (fn: (v: boolean) => boolean) => void;
  dark: boolean;
  highlightMode: HighlightMode;
  onSpanClick: (v: string) => void;
  styles: DetailStyles;
}>) {
  const { headerCls, borderCls, c } = styles;
  return (
    <>
      <tr>
        <th colSpan={2} className={headerCls}>
          <span className="flex items-center gap-2">
            {`Message (${formatBytes(rawBytes)})`}
            <CopyButton text={rawText} dark={dark} />
          </span>
        </th>
      </tr>
      <tr>
        <td colSpan={2} className={`pb-1 max-w-0 ${borderCls}`}>
          <pre
            className={`text-[0.85em] font-mono p-3 rounded border whitespace-pre overflow-x-auto leading-relaxed app-scroll ${c("border-ink-border-subtle bg-ink text-text-normal", "border-light-border-subtle bg-light-bg text-light-text-normal")}`}
            onClick={(e) => {
              if (!e.altKey) return;
              const el = (e.target as HTMLElement).closest<HTMLElement>("[data-click-value]");
              if (el) {
                e.stopPropagation();
                onSpanClick(el.dataset.clickValue!);
              }
            }}
          >
            {(() => {
              const spans = syntaxHighlight(visibleText, highlightMode);
              const offsets: number[] = [];
              for (let j = 0; j < spans.length; j++) {
                offsets.push(j === 0 ? 0 : offsets[j - 1]! + spans[j - 1]!.text.length);
              }
              return spans.map((span, i) => {
                const style = span.color ? { color: span.color } : undefined;
                const key = `msg-${offsets[i]}`;
                if (span.url) {
                  return (
                    <a
                      key={key}
                      href={span.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={style}
                      className="underline decoration-current/30 hover:decoration-current/60"
                    >
                      {span.text}
                    </a>
                  );
                }
                if (span.clickValue) {
                  return (
                    <span
                      key={key}
                      style={style}
                      className="cursor-pointer hover:brightness-125"
                      data-click-value={span.clickValue}
                      title="⌥ click to add filter"
                    >
                      {span.text}
                    </span>
                  );
                }
                return (
                  <span key={key} style={style}>
                    {span.text}
                  </span>
                );
              });
            })()}
          </pre>
          {isLarge && (
            <button
              type="button"
              onClick={() => setCollapsed((v) => !v)}
              className={`mt-1 text-[0.8em] font-mono px-2 py-0.5 rounded transition-colors ${c("text-copper hover:bg-copper/10", "text-copper hover:bg-copper/10")}`}
            >
              {collapsed
                ? `Show all (${displayLineCount.toLocaleString()} lines)`
                : "Collapse"}
            </button>
          )}
        </td>
      </tr>
    </>
  );
}

function ReferenceSection({
  record,
  onVaultSelect,
  onChunkSelect,
  onPosSelect,
  styles,
}: Readonly<{
  record: ProtoRecord;
  onVaultSelect: (vaultId: string) => void;
  onChunkSelect: (chunkId: string) => void;
  onPosSelect: (chunkId: string, pos: string) => void;
  styles: DetailStyles;
}>) {
  const { headerCls, keyCls, borderCls } = styles;
  const { data: config } = useConfig();
  const vaultId = record.ref?.vaultId ?? "";
  const vaultName = config?.vaults.find((v) => v.id === vaultId)?.name;

  return (
    <>
      <tr>
        <th colSpan={2} className={headerCls}>Reference</th>
      </tr>
      <tr>
        <td className={`${keyCls} ${borderCls}`}>vault_id</td>
        <ValueCell
          value={vaultId || "N/A"}
          onClick={vaultId ? () => onVaultSelect(vaultId) : undefined}
          styles={styles}
        />
      </tr>
      {vaultName && (
        <tr>
          <td className={`${keyCls} ${borderCls}`}>vault</td>
          <ValueCell
            value={vaultName}
            onClick={() => onVaultSelect(vaultId)}
            styles={styles}
          />
        </tr>
      )}
      <tr>
        <td className={`${keyCls} ${borderCls}`}>chunk_id</td>
        <ValueCell
          value={record.ref?.chunkId ? formatChunkId(record.ref.chunkId) : "N/A"}
          onClick={record.ref?.chunkId ? () => onChunkSelect(record.ref!.chunkId) : undefined}
          styles={styles}
        />
      </tr>
      <tr>
        <td className={`${keyCls} ${borderCls}`}>pos</td>
        <ValueCell
          value={record.ref?.pos.toString() ?? "N/A"}
          onClick={record.ref?.chunkId
            ? () => onPosSelect(record.ref!.chunkId, record.ref!.pos.toString())
            : undefined}
          styles={styles}
        />
      </tr>
    </>
  );
}

export function DetailPanelContent({
  record,
  dark,
}: Readonly<{
  record: ProtoRecord;
  dark: boolean;
}>) {
  const {
    onFieldSelect,
    onMultiFieldSelect,
    onSpanClick,
    onChunkSelect,
    onVaultSelect,
    onPosSelect,
    contextBefore,
    contextAfter,
    contextLoading,
    contextReversed,
    onContextRecordSelect,
    highlightMode,
  } = useDetailPanel();
  const c = useThemeClass(dark);
  const rawText = new TextDecoder().decode(record.raw);
  const rawBytes = record.raw.length;

  let displayText = rawText;
  if (rawText.trimStart().startsWith("{")) {
    try {
      displayText = JSON.stringify(JSON.parse(rawText), null, 2);
    } catch {
      // Not valid JSON — use raw text as-is.
    }
  }
  const displayLines = displayText.split("\n");
  const isLarge = displayLines.length > MAX_DISPLAY_LINES;
  const [collapsed, setCollapsed] = useState(true);
  const visibleText =
    isLarge && collapsed
      ? displayLines.slice(0, MAX_DISPLAY_LINES).join("\n")
      : displayText;

  const kvPairs = extractKVPairs(rawText);

  const tsRows = [
    { label: "Write", ts: record.writeTs },
    { label: "Ingest", ts: record.ingestTs },
    { label: "Source", ts: record.sourceTs },
  ];

  const styles = buildStyles(c);
  const { headerCls, keyCls, borderCls } = styles;

  return (
    <div className="p-4">
      <table className="w-full border-collapse">
        <tbody>
          {/* — Timestamps — */}
          <tr>
            <th colSpan={2} className={headerCls}>Timestamps</th>
          </tr>
          {tsRows.map(({ label, ts }) => {
            const instant = ts ? protoToInstant(ts) : null;
            return (
              <tr key={label}>
                <td className={`${keyCls} ${borderCls}`}>{label}</td>
                <td className={styles.valCls}>
                  {instant ? (
                    <>
                      <div className={`break-all ${c("text-text-normal", "text-light-text-normal")}`}>
                        {instantToISO(instant)}
                      </div>
                      <div className={`text-[0.7em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
                        {relativeTime(instant)}
                      </div>
                    </>
                  ) : (
                    <div className={c("text-text-ghost", "text-light-text-ghost")}>
                      {"\u2014"}
                    </div>
                  )}
                </td>
              </tr>
            );
          })}

          <MessageSection
            rawText={rawText}
            rawBytes={rawBytes}
            visibleText={visibleText}
            isLarge={isLarge}
            displayLineCount={displayLines.length}
            collapsed={collapsed}
            setCollapsed={setCollapsed}
            dark={dark}
            highlightMode={highlightMode}
            onSpanClick={onSpanClick}
            styles={styles}
          />

          {/* — Attributes — */}
          {Object.keys(record.attrs).length > 0 && (
            <>
              <tr>
                <th colSpan={2} className={headerCls}>Attributes</th>
              </tr>
              {Object.entries(record.attrs).map(([k, v]) => (
                <tr key={k}>
                  <td className={`${keyCls} ${borderCls}`}>{k}</td>
                  <ValueCell value={v} onClick={() => onFieldSelect(k, v)} styles={styles} />
                </tr>
              ))}
            </>
          )}

          {/* — Extracted Fields — */}
          {kvPairs.length > 0 && (
            <>
              <tr>
                <th colSpan={2} className={headerCls}>Extracted Fields</th>
              </tr>
              {kvPairs.map(({ key, value }) => (
                <tr key={`kv-${key}-${value}`}>
                  <td className={`${keyCls} ${borderCls}`}>{key}</td>
                  <ValueCell value={value} onClick={() => onFieldSelect(key, value)} styles={styles} />
                </tr>
              ))}
            </>
          )}

          <ReferenceSection
            record={record}
            onVaultSelect={onVaultSelect}
            onChunkSelect={onChunkSelect}
            onPosSelect={onPosSelect}
            styles={styles}
          />

          {/* — Event Identity — */}
          <tr>
            <th colSpan={2} className={headerCls}>
              {onMultiFieldSelect ? (
                <button
                  type="button"
                  title="Filter by this event identity"
                  className={`cursor-pointer uppercase transition-colors ${c("hover:text-copper", "hover:text-copper")}`}
                  onClick={() => {
                    onMultiFieldSelect([
                      ["ingester_id", formatUUID(record.ingesterId)],
                      ["ingest_seq", record.ingestSeq.toString()],
                      ["ingest_ts", instantToISO(protoToInstant(record.ingestTs!))],
                    ]);
                  }}
                >
                  Event Identity
                </button>
              ) : (
                "Event Identity"
              )}
            </th>
          </tr>
          <tr>
            <td className={`${keyCls} ${borderCls}`}>ingester_id</td>
            <ValueCell
              value={formatUUID(record.ingesterId)}
              onClick={() => onFieldSelect("ingester_id", formatUUID(record.ingesterId))}
              styles={styles}
            />
          </tr>
          <tr>
            <td className={`${keyCls} ${borderCls}`}>ingest_seq</td>
            <ValueCell
              value={record.ingestSeq.toString()}
              onClick={() => onFieldSelect("ingest_seq", record.ingestSeq.toString())}
              styles={styles}
            />
          </tr>

          {/* — Context — */}
          <tr>
            <th colSpan={2} className={headerCls}>Context</th>
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
  return records?.map((rec) => (
    <ContextRecord
      key={`${prefix}-${rec.ref?.chunkId}-${rec.ref?.pos}`}
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
  const before = contextReversed ? contextAfter?.toReversed() : contextBefore;
  const after = contextReversed ? contextBefore?.toReversed() : contextAfter;
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
