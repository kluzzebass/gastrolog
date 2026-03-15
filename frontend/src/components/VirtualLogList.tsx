import type { RefObject } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Record as ProtoRecord } from "../api/client";
import { sameRecord } from "../utils";
import { LogEntry, type OrderByTS } from "./LogEntry";
import type { HighlightMode } from "../hooks/useThemeSync";

interface VirtualLogListProps {
  records: ProtoRecord[];
  selectedRecord: ProtoRecord | null;
  tokens: string[];
  highlightMode: HighlightMode;
  dark: boolean;
  scrollRef: RefObject<HTMLDivElement | null>;
  selectedRowRef: RefObject<HTMLElement | null>;
  onSelectRecord: (rec: ProtoRecord | null) => void;
  onTokenToggle: (token: string) => void;
  onSpanClick: (value: string) => void;
  orderBy: OrderByTS;
}

export function VirtualLogList({
  records,
  selectedRecord,
  tokens,
  highlightMode,
  dark,
  scrollRef,
  selectedRowRef,
  onSelectRecord,
  onTokenToggle,
  onSpanClick,
  orderBy,
}: Readonly<VirtualLogListProps>) {
  // eslint-disable-next-line react-hooks/incompatible-library -- third-party library
  const virtualizer = useVirtualizer({
    count: records.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 40,
    overscan: 20,
  });

  const items = virtualizer.getVirtualItems();

  return (
    <div
      style={{
        height: virtualizer.getTotalSize(),
        width: "100%",
        position: "relative",
      }}
    >
      {items.map((virtualRow) => {
        const record = records[virtualRow.index]!;
        const selected = sameRecord(selectedRecord, record);
        return (
          <div
            key={virtualRow.key}
            ref={virtualizer.measureElement}
            data-index={virtualRow.index}
            style={{
              position: "absolute",
              top: 0,
              left: 0,
              width: "100%",
              transform: `translateY(${virtualRow.start}px)`,
            }}
          >
            <LogEntry
              ref={selected ? selectedRowRef : undefined}
              record={record}
              tokens={tokens}
              isSelected={selected}
              onSelect={() => onSelectRecord(selected ? null : record)}
              onFilterToggle={onTokenToggle}
              onSpanClick={onSpanClick}
              dark={dark}
              highlightMode={highlightMode}
              orderBy={orderBy}
              rowIndex={virtualRow.index}
            />
          </div>
        );
      })}
    </div>
  );
}
