import { useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useThemeClass } from "../../hooks/useThemeClass";

export type TooltipData = {
  title: string;
  items: { color: string; label: string; value: string }[];
  /** When set, the matching item label is bolded and others are dimmed. */
  highlightLabel?: string;
};

/**
 * Tooltip hook that updates position via direct DOM manipulation (no re-render)
 * and only triggers React re-renders when tooltip content actually changes.
 */
export function useChartTooltip() {
  const elRef = useRef<HTMLDivElement | null>(null);
  const posRef = useRef({ x: 0, y: 0 });
  const [data, setData] = useState<TooltipData | null>(null);

  const applyPosition = (el: HTMLDivElement, cx: number, cy: number) => {
    const gap = 12;
    const w = el.offsetWidth;
    const h = el.offsetHeight;
    const left = cx + gap + w > window.innerWidth ? cx - gap - w : cx + gap;
    const top = cy + gap + h > window.innerHeight ? cy - gap - h : cy + gap;
    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
  };

  const tooltipRef = (el: HTMLDivElement | null) => {
    elRef.current = el;
    if (el) applyPosition(el, posRef.current.x, posRef.current.y);
  };

  const showTooltip = ({
    tooltipData,
    tooltipLeft,
    tooltipTop,
  }: {
    tooltipData: TooltipData;
    tooltipLeft: number;
    tooltipTop: number;
  }) => {
    // Position: direct DOM — no re-render, instant movement.
    posRef.current = { x: tooltipLeft, y: tooltipTop };
    if (elRef.current) applyPosition(elRef.current, tooltipLeft, tooltipTop);
    // Data: React state — only re-render when content actually changed.
    setData((prev) => {
      if (
        prev &&
        prev.title === tooltipData.title &&
        prev.highlightLabel === tooltipData.highlightLabel &&
        prev.items.length === tooltipData.items.length &&
        prev.items.every((it, i) => it.value === tooltipData.items[i]?.value && it.label === tooltipData.items[i]?.label)
      ) {
        return prev;
      }
      return tooltipData;
    });
  };

  const hideTooltip = () => setData(null);

  /** Update tooltip content without changing position (e.g. when underlying data refreshes). */
  const updateData = (tooltipData: TooltipData) => setData(tooltipData);

  return {
    tooltipOpen: data !== null,
    tooltipData: data,
    tooltipRef,
    showTooltip,
    hideTooltip,
    updateData,
  };
}

export function ChartTooltip({
  tooltipRef,
  data,
  dark,
}: Readonly<{
  tooltipRef: (el: HTMLDivElement | null) => void;
  data: TooltipData;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const hl = data.highlightLabel;
  return createPortal(
    <div
      ref={tooltipRef}
      className={`fixed pointer-events-none px-2 py-1 rounded text-[0.75em] font-mono whitespace-nowrap z-50 ${c(
        "bg-ink-surface text-text-bright border border-ink-border-subtle",
        "bg-light-surface text-light-text-bright border border-light-border-subtle",
      )}`}
    >
      <div className={c("text-text-ghost", "text-light-text-ghost")}>
        {data.title}
      </div>
      {data.items.map((item) => {
        const isHighlighted = hl != null && item.label === hl;
        const isDimmed = hl != null && item.label !== hl;
        return (
          <div
            key={item.label}
            className={`flex items-center gap-1.5 ${isHighlighted ? "font-bold" : ""}`}
          >
            <span
              className="inline-block w-1.5 h-1.5 rounded-full shrink-0"
              style={{ backgroundColor: item.color }}
            />
            {data.items.length > 1 && (
              <span className={isDimmed ? "opacity-50" : isHighlighted ? "" : "opacity-70"}>
                {item.label}
              </span>
            )}
            <span>{item.value}</span>
          </div>
        );
      })}
    </div>,
    document.body,
  );
}
