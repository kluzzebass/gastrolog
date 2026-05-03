import { useRef } from "react";
import type { HistogramData } from "../utils/histogramData";

// ── Interaction state reducer ─────────────────────────────────────────

export interface ChartInteraction {
  hoveredGroup: string | null;
  hoveredBar: number | null;
  brushStart: number | null;
  brushEnd: number | null;
  panAxisWidth: number;
  panOffset: number;
}

export type ChartAction =
  | { type: "hover"; group: string | null; bar: number | null }
  | { type: "brushStart"; idx: number }
  | { type: "brushMove"; idx: number }
  | { type: "brushEnd" }
  | { type: "panStart"; axisWidth: number }
  | { type: "panMove"; offset: number }
  | { type: "panEnd" };

export const CHART_INITIAL: ChartInteraction = {
  hoveredGroup: null, hoveredBar: null,
  brushStart: null, brushEnd: null,
  panAxisWidth: 1, panOffset: 0,
};

export function chartReducer(state: ChartInteraction, action: ChartAction): ChartInteraction {
  switch (action.type) {
    case "hover":
      return { ...state, hoveredGroup: action.group, hoveredBar: action.bar };
    case "brushStart":
      return { ...state, brushStart: action.idx, brushEnd: action.idx };
    case "brushMove":
      return { ...state, brushEnd: action.idx };
    case "brushEnd":
      return { ...state, brushStart: null, brushEnd: null };
    case "panStart":
      return { ...state, panAxisWidth: action.axisWidth };
    case "panMove":
      return { ...state, panOffset: action.offset };
    case "panEnd":
      return { ...state, panOffset: 0 };
  }
}

/** Set grab cursor on document body for pan dragging. */
function setGrabbingCursor() {
  document.body.style.cursor = "grabbing";
  document.body.style.userSelect = "none";
}

/** Reset document body cursor after pan dragging. */
function clearGrabbingCursor() {
  document.body.style.cursor = "";
  document.body.style.userSelect = "";
}

interface HistogramInteractionDeps {
  buckets: HistogramData["buckets"];
  firstBucket: HistogramData["buckets"][0] | undefined;
  lastBucket: HistogramData["buckets"][0] | undefined;
  chartContainerRef: React.RefObject<HTMLDivElement | null>;
  axisRef: React.RefObject<HTMLDivElement | null>;
  ix: ChartInteraction;
  dix: React.Dispatch<ChartAction>;
  onBrushSelect?: (start: Date, end: Date) => void;
  onPan?: (start: Date, end: Date) => void;
  onSegmentClick?: (level: string) => void;
}

interface HistogramInteractionResult {
  onEvents: Record<string, (params: any) => void>;
  handleMouseDown: (e: React.MouseEvent) => void;
  brushLo: number | null;
  brushHi: number | null;
  handlePanStep: (direction: -1 | 1) => void;
  handleAxisMouseDown: (e: React.MouseEvent) => void;
  labelCount: number;
  labelStep: number;
  panDeltaMs: number;
  formatDuration: (ms: number) => string;
}

export function useHistogramInteraction(deps: HistogramInteractionDeps): HistogramInteractionResult {
  const brushingRef = useRef(false);
  const panStartX = useRef(0);
  const panningRef = useRef(false);

  const {
    buckets,
    firstBucket,
    lastBucket,
    chartContainerRef,
    axisRef,
    ix,
    dix,
    onBrushSelect,
    onPan,
    onSegmentClick,
  } = deps;

  // ECharts event handlers for hover highlighting and segment click.
  const onEvents = {
    mouseover: (params: any) => {
      const name = params.seriesName as string;
      const group = name === "count" ? null : name;
      dix({ type: "hover", group, bar: params.dataIndex as number });
    },
    mouseout: () => {
      dix({ type: "hover", group: null, bar: null });
    },
    click: (params: any) => {
      if (!onSegmentClick) return;
      const name = params.seriesName as string;
      if (name !== "count") {
        onSegmentClick(name);
      }
    },
  };

  // Brush helpers — use the chart container div for hit-testing.
  const getBucketIndex = (clientX: number): number => {
    const el = chartContainerRef.current;
    if (!el) return 0;
    const rect = el.getBoundingClientRect();
    const x = clientX - rect.left;
    const idx = Math.floor((x / rect.width) * buckets.length);
    return Math.max(0, Math.min(buckets.length - 1, idx));
  };

  const handleMouseDown = (e: React.MouseEvent) => {
    if (!onBrushSelect) return;
    if (e.button !== 0) return;
    e.preventDefault();
    const idx = getBucketIndex(e.clientX);
    dix({ type: "brushStart", idx });
    brushingRef.current = true;

    const onMouseMove = (ev: MouseEvent) => {
      if (!brushingRef.current) return;
      dix({ type: "brushMove", idx: getBucketIndex(ev.clientX) });
    };
    const onMouseUp = (ev: MouseEvent) => {
      if (!brushingRef.current) return;
      brushingRef.current = false;
      const endIdx = getBucketIndex(ev.clientX);
      const lo = Math.min(idx, endIdx);
      const hi = Math.max(idx, endIdx);
      if (lo !== hi) {
        onBrushSelect(buckets[lo]!.ts, buckets[hi]!.ts);
      }
      dix({ type: "brushEnd" });
      globalThis.removeEventListener("mousemove", onMouseMove);
      globalThis.removeEventListener("mouseup", onMouseUp);
    };
    globalThis.addEventListener("mousemove", onMouseMove, { passive: true });
    globalThis.addEventListener("mouseup", onMouseUp);
  };

  const brushLo =
    ix.brushStart !== null && ix.brushEnd !== null
      ? Math.min(ix.brushStart, ix.brushEnd)
      : null;
  const brushHi =
    ix.brushStart !== null && ix.brushEnd !== null
      ? Math.max(ix.brushStart, ix.brushEnd)
      : null;

  // Pan handlers.
  const handlePanStep = (direction: -1 | 1) => {
    if (!onPan || buckets.length < 2) return;
    const windowMs = lastBucket!.ts.getTime() - firstBucket!.ts.getTime();
    const stepMs = windowMs / 2;
    const first = firstBucket!.ts.getTime();
    const last = lastBucket!.ts.getTime();
    onPan(
      new Date(first + direction * stepMs),
      new Date(last + direction * stepMs),
    );
  };

  const handleAxisMouseDown = (e: React.MouseEvent) => {
    if (!onPan || buckets.length < 2) return;
    e.preventDefault();
    panStartX.current = e.clientX;
    dix({ type: "panStart", axisWidth: axisRef.current?.getBoundingClientRect().width || 1 });
    panningRef.current = true;
    setGrabbingCursor();

    const onMouseMove = (ev: MouseEvent) => {
      dix({ type: "panMove", offset: ev.clientX - panStartX.current });
    };
    const onMouseUp = (ev: MouseEvent) => {
      panningRef.current = false;
      clearGrabbingCursor();
      dix({ type: "panEnd" });
      globalThis.removeEventListener("mousemove", onMouseMove);
      globalThis.removeEventListener("mouseup", onMouseUp);

      const el = axisRef.current;
      if (!el) return;
      const deltaX = panStartX.current - ev.clientX;
      const axisWidth = el.getBoundingClientRect().width;
      if (Math.abs(deltaX) < 3) return;
      const windowMs = lastBucket!.ts.getTime() - firstBucket!.ts.getTime();
      const deltaMs = (deltaX / axisWidth) * windowMs;
      const first = firstBucket!.ts.getTime();
      const last = lastBucket!.ts.getTime();
      onPan(new Date(first + deltaMs), new Date(last + deltaMs));
    };
    globalThis.addEventListener("mousemove", onMouseMove, { passive: true });
    globalThis.addEventListener("mouseup", onMouseUp);
  };

  const labelCount = Math.min(5, buckets.length);
  // Span the full bucket range so the first AND last bucket anchor the axis.
  // floor(buckets.length / labelCount) lands the last tick well short of the
  // right edge — with 50 buckets and labelCount=5 the rightmost tick was
  // bucket 40, so a span ending at bucket 49 looked truncated even though
  // the data went all the way to it.
  const labelStep =
    labelCount > 1 ? (buckets.length - 1) / (labelCount - 1) : 0;

  const windowMs =
    buckets.length > 1 ? lastBucket!.ts.getTime() - firstBucket!.ts.getTime() : 0;
  const panDeltaMs =
    ix.panOffset !== 0 ? -((ix.panOffset / ix.panAxisWidth) * windowMs) : 0;

  const formatDuration = (ms: number): string => {
    const abs = Math.abs(ms);
    const sign = ms < 0 ? "-" : "+";
    if (abs < 1000) return `${sign}${Math.round(abs)}ms`;
    if (abs < 60_000) return `${sign}${(abs / 1000).toFixed(1)}s`;
    if (abs < 3_600_000) {
      const m = Math.floor(abs / 60_000);
      const s = Math.round((abs % 60_000) / 1000);
      return s > 0 ? `${sign}${m}m ${s}s` : `${sign}${m}m`;
    }
    if (abs < 86_400_000) {
      const h = Math.floor(abs / 3_600_000);
      const m = Math.round((abs % 3_600_000) / 60_000);
      return m > 0 ? `${sign}${h}h ${m}m` : `${sign}${h}h`;
    }
    const d = Math.floor(abs / 86_400_000);
    const h = Math.round((abs % 86_400_000) / 3_600_000);
    return h > 0 ? `${sign}${d}d ${h}h` : `${sign}${d}d`;
  };

  return {
    onEvents,
    handleMouseDown,
    brushLo,
    brushHi,
    handlePanStep,
    handleAxisMouseDown,
    labelCount,
    labelStep,
    panDeltaMs,
    formatDuration,
  };
}
