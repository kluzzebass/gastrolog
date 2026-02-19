import { Record as ProtoRecord } from "../api/client";
import { DetailPanelContent } from "./DetailPanel";
import { useThemeClass } from "../hooks/useThemeClass";

interface DetailSidebarProps {
  dark: boolean;
  detailWidth: number;
  detailCollapsed: boolean;
  setDetailCollapsed: (v: boolean) => void;
  detailPinned: boolean;
  setDetailPinned: (v: boolean | ((p: boolean) => boolean)) => void;
  handleDetailResize: (e: React.MouseEvent) => void;
  resizing: boolean;
  selectedRecord: ProtoRecord | null;
  onFieldSelect: (key: string, value: string) => void;
  onChunkSelect: (chunkId: string) => void;
  onStoreSelect: (storeId: string) => void;
  onPosSelect: (chunkId: string, pos: string) => void;
  contextBefore: ProtoRecord[];
  contextAfter: ProtoRecord[];
  contextLoading: boolean;
  contextReversed: boolean;
  onContextRecordSelect: (rec: ProtoRecord) => void;
}

export function DetailSidebar({
  dark,
  detailWidth,
  detailCollapsed,
  setDetailCollapsed,
  detailPinned,
  setDetailPinned,
  handleDetailResize,
  resizing,
  selectedRecord,
  onFieldSelect,
  onChunkSelect,
  onStoreSelect,
  onPosSelect,
  contextBefore,
  contextAfter,
  contextLoading,
  contextReversed,
  onContextRecordSelect,
}: Readonly<DetailSidebarProps>) {
  const c = useThemeClass(dark);

  return (
    <>
      {/* Detail resize handle + collapse toggle */}
      {!detailCollapsed && (
        <div className="relative shrink-0 flex">
          <button
            onClick={() => setDetailCollapsed(true)}
            className={`absolute top-2 -left-3 w-4 h-6 flex items-center justify-center text-[0.6em] rounded-l z-10 transition-colors ${c(
              "bg-ink-surface border border-r-0 border-ink-border-subtle text-text-ghost hover:text-text-muted",
              "bg-light-surface border border-r-0 border-light-border-subtle text-light-text-ghost hover:text-light-text-muted",
            )}`}
            aria-label="Collapse detail panel"
            title="Collapse detail panel"
          >
            {"\u25B8"}
          </button>
          <div
            onMouseDown={handleDetailResize}
            className={`w-1 cursor-col-resize transition-colors ${c("hover:bg-copper-muted/30", "hover:bg-copper-muted/20")}`}
          />
        </div>
      )}
      {detailCollapsed && (
        <button
          onClick={() => setDetailCollapsed(false)}
          className={`shrink-0 px-1 flex items-center border-l transition-colors ${c(
            "border-ink-border-subtle bg-ink-surface text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "border-light-border-subtle bg-light-surface text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )}`}
          aria-label="Expand detail panel"
          title="Expand detail panel"
        >
          {"\u25C2"}
        </button>
      )}
      <aside
        aria-label="Record details"
        style={{ width: detailCollapsed ? 0 : detailWidth }}
        className={`shrink-0 overflow-hidden ${resizing ? "" : "transition-[width] duration-200"} ${
          detailCollapsed
            ? ""
            : `border-l overflow-y-auto app-scroll ${c("border-ink-border-subtle bg-ink-surface", "border-light-border-subtle bg-light-surface")}`
        }`}
      >
        <div
          className={`flex items-center justify-between px-4 py-3 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
        >
          <h3
            className={`font-display text-[1.15em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Details
          </h3>
          <button
            onClick={() => {
              setDetailPinned((p: boolean) => !p);
              if (detailPinned && !selectedRecord) setDetailCollapsed(true);
            }}
            aria-label={detailPinned ? "Unpin detail panel" : "Pin detail panel"}
            title={detailPinned ? "Unpin detail panel" : "Pin detail panel"}
            className={`w-6 h-6 flex items-center justify-center rounded transition-colors ${
              detailPinned
                ? c("text-copper", "text-copper")
                : c(
                    "text-text-ghost hover:text-text-muted",
                    "text-light-text-ghost hover:text-light-text-muted",
                  )
            }`}
          >
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="w-4 h-4"
              style={
                detailPinned ? undefined : { transform: "rotate(45deg)" }
              }
            >
              <line x1="12" y1="17" x2="12" y2="22" />
              <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6h1a2 2 0 0 0 0-4H8a2 2 0 0 0 0 4h1v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z" />
            </svg>
          </button>
        </div>

        {selectedRecord ? (
          <DetailPanelContent
            record={selectedRecord}
            dark={dark}
            onFieldSelect={onFieldSelect}
            onChunkSelect={onChunkSelect}
            onStoreSelect={onStoreSelect}
            onPosSelect={onPosSelect}
            contextBefore={contextBefore}
            contextAfter={contextAfter}
            contextLoading={contextLoading}
            contextReversed={contextReversed}
            onContextRecordSelect={onContextRecordSelect}
          />
        ) : (
          <div className="flex flex-col items-center justify-center h-48 px-4">
            <p
              className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              Select a record to view details
            </p>
          </div>
        )}
      </aside>
    </>
  );
}
