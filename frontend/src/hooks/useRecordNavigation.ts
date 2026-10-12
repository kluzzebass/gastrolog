/**
 * Record selection, keyboard navigation, and detail panel auto-expand.
 * Extracted from useSearchView to isolate selection state from search execution.
 */
import { useState, useEffect, useEffectEvent } from "react";
import type { MutableRefObject } from "react";
import { Record as ProtoRecord } from "../api/client";
import type { RecordRef } from "../api/gen/gastrolog/v1/query_pb";
import { sameRecord } from "../utils";

interface RecordNavigationDeps {
  isFollowMode: boolean;
  records: ProtoRecord[];
  followRecords: ProtoRecord[];
  selectedRowRef: MutableRefObject<HTMLElement | null>;

  // Detail panel state (owned by usePanelLayout, passed through)
  detailCollapsed: boolean;
  setDetailCollapsed: (v: boolean) => void;
  detailPinned: boolean;

  // Dialog state
  showPlan: boolean;
  setShowPlan: (v: boolean) => void;

  // Context fetching
  fetchContext: (ref: RecordRef, before?: number, after?: number) => Promise<void>;
  resetContext: () => void;
}

export function useRecordNavigation(deps: RecordNavigationDeps) {
  const {
    isFollowMode, records, followRecords, selectedRowRef,
    detailCollapsed, setDetailCollapsed, detailPinned,
    showPlan, setShowPlan,
    fetchContext, resetContext,
  } = deps;

  const [selectedRecord, setSelectedRecord] = useState<ProtoRecord | null>(null);

  // Auto-expand detail panel and fetch context when a record is selected.
  useEffect(() => {
    if (selectedRecord && detailCollapsed) setDetailCollapsed(false);
    if (!selectedRecord && !detailPinned) setDetailCollapsed(true);
    if (selectedRecord?.ref) {
      fetchContext(selectedRecord.ref);
    } else {
      resetContext();
    }
    if (selectedRecord) {
      requestAnimationFrame(() => {
        selectedRowRef.current?.scrollIntoView({ block: "nearest" });
      });
    }
  }, [selectedRecord]); // eslint-disable-line react-hooks/exhaustive-deps

  const navigateRecords = (e: KeyboardEvent) => {
    const tag = (e.target as HTMLElement).tagName;
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;

    const list = isFollowMode ? followRecords : records;
    if (list.length === 0) return;

    e.preventDefault();
    const dir = e.key === "ArrowUp" ? -1 : 1;
    if (!selectedRecord) {
      setSelectedRecord(dir === 1 ? list[0]! : list.at(-1)!);
      return;
    }
    const idx = list.findIndex((r) => sameRecord(r, selectedRecord));
    if (idx === -1) {
      setSelectedRecord(dir === 1 ? list[0]! : list.at(-1)!);
      return;
    }
    const next = idx + dir;
    if (next >= 0 && next < list.length) {
      setSelectedRecord(list[next]!);
    }
  };

  const onKeyDown = useEffectEvent((e: KeyboardEvent) => {
    if (e.key === "Escape") {
      if (showPlan) {
        setShowPlan(false);
        return;
      }
      setSelectedRecord(null);
      if (!detailPinned) setDetailCollapsed(true);
      return;
    }

    if (e.key === "ArrowUp" || e.key === "ArrowDown") {
      navigateRecords(e);
    }
  });

  // Global keyboard shortcuts: Escape to deselect, Arrow keys to navigate records.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => onKeyDown(e);
    globalThis.addEventListener("keydown", handler);
    return () => globalThis.removeEventListener("keydown", handler);
  }, []);

  return { selectedRecord, setSelectedRecord };
}
