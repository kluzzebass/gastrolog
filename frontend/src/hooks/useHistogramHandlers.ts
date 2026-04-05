/**
 * Histogram interaction handlers extracted from useSearchView.
 * Brush select, follow brush, pan, zoom, and context record navigation.
 */
import { Record as ProtoRecord } from "../api/client";
import { protoToInstant, instantToMs } from "../utils/temporal";
import { stripTimeRange } from "../utils/queryHelpers";

interface HistogramHandlerDeps {
  q: string;
  isReversed: boolean;
  setUrlQuery: (newQ: string) => void;
  navigate: (opts: {
    to?: string;
    search?: Record<string, string | undefined> | ((prev: Record<string, string | undefined>) => Record<string, string | undefined>);
    replace?: boolean;
  }) => void;

  // Time range state
  rangeStart: Date | null;
  rangeEnd: Date | null;
  setTimeRange: (v: string) => void;
  setRangeStart: (v: Date) => void;
  setRangeEnd: (v: Date) => void;

  // Record selection (for zoom anchor)
  selectedRecord: ProtoRecord | null;
  setSelectedRecord: (r: ProtoRecord | null) => void;
}

export function useHistogramHandlers(deps: HistogramHandlerDeps) {
  const {
    q, isReversed, setUrlQuery, navigate,
    rangeStart, rangeEnd, setTimeRange, setRangeStart, setRangeEnd,
    selectedRecord, setSelectedRecord,
  } = deps;

  const handleBrushSelect = (start: Date, end: Date) => {
    setRangeStart(start);
    setRangeEnd(end);
    setTimeRange("custom");
    const rangeTokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=${isReversed}`;
    const base = stripTimeRange(q);
    const newQuery = base ? `${rangeTokens} ${base}` : rangeTokens;
    setUrlQuery(newQuery);
  };

  const handleFollowBrushSelect = (start: Date, end: Date) => {
    setRangeStart(start);
    setRangeEnd(end);
    setTimeRange("custom");
    const rangeTokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=true`;
    const base = stripTimeRange(q);
    const newQuery = base ? `${rangeTokens} ${base}` : rangeTokens;
    navigate({
      to: "/search",
      search: (prev) => ({ ...prev, q: newQuery }),
      replace: false,
    });
  };

  const handlePan = (start: Date, end: Date) => {
    handleBrushSelect(start, end);
  };

  const handleZoomOut = () => {
    const anchorMs = selectedRecord?.writeTs ? instantToMs(protoToInstant(selectedRecord.writeTs)) : 0;
    if (!anchorMs) return;
    const curStart = rangeStart?.getTime() ?? anchorMs - 30_000;
    const curEnd = rangeEnd?.getTime() ?? anchorMs + 30_000;
    const span = curEnd - curStart;
    const mid = anchorMs;
    const newStart = new Date(mid - span);
    const newEnd = new Date(mid + span);
    setTimeRange("custom");
    setRangeStart(newStart);
    setRangeEnd(newEnd);
    const newQuery = `start=${newStart.toISOString()} end=${newEnd.toISOString()} reverse=${isReversed}`;
    navigate({
      to: "/search",
      search: (prev) => ({ ...prev, q: newQuery }),
      replace: false,
    });
  };

  const handleContextRecordSelect = (rec: ProtoRecord) => {
    const tsMs = rec.writeTs ? instantToMs(protoToInstant(rec.writeTs)) : 0;
    if (tsMs) {
      const start = new Date(tsMs - 30_000);
      const end = new Date(tsMs + 30_000);
      const newQuery = `start=${start.toISOString()} end=${end.toISOString()} reverse=true`;
      setTimeRange("custom");
      setRangeStart(start);
      setRangeEnd(end);
      setSelectedRecord(rec);
      navigate({
        to: "/search",
        search: (prev) => ({ ...prev, q: newQuery }),
        replace: false,
      });
    }
  };

  return {
    handleBrushSelect,
    handleFollowBrushSelect,
    handlePan,
    handleZoomOut,
    handleContextRecordSelect,
  };
}
