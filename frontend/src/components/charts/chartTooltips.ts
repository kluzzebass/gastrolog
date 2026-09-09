/**
 * Every ECharts tooltip HTML string in the app is built here.
 *
 * ECharts inserts a `formatter` string result into the tooltip DOM as HTML, so
 * the values these builders interpolate — category names, series names, column
 * headers — are a script-execution surface. They originate in log records,
 * which no ingester authenticates. Keeping the construction in one module makes
 * the rule checkable: every interpolation below goes through `escapeHtml`.
 */
import { escapeHtml } from "../../lib/escapeHtml";
import { formatChartValue, resolveColor } from "./chartColors";
import type { HistogramData } from "../../utils/histogramData";

/** Colored round series marker. The margin matches the diameter. */
function dot(color: string, size: number): string {
  return `<span style="display:inline-block;width:${size}px;height:${size}px;border-radius:50%;background:${escapeHtml(color)};margin-right:${size}px;"></span>`;
}

/** Muted first line of a tooltip. */
function header(text: string): string {
  return `<div style="opacity:0.7">${escapeHtml(text)}</div>`;
}

export function barTooltipHtml(params: any, columns: string[]): string {
  const p = Array.isArray(params) ? params[0] : params;
  const valueCol = columns.at(-1);
  return `${header(p.name)}${dot(p.color as string, 6)}${escapeHtml(valueCol)} <b>${escapeHtml(formatChartValue(p.value as number))}</b>`;
}

export function donutTooltipHtml(params: any, columns: string[], total: number): string {
  const value = params.value as number;
  const pct = ((value / total) * 100).toFixed(1);
  const valueCol = columns.at(-1);
  return `${header(params.name)}${dot(params.color as string, 6)}${escapeHtml(valueCol)} <b>${escapeHtml(formatChartValue(value))} (${escapeHtml(pct)}%)</b>`;
}

export function heatmapTooltipHtml(
  params: any,
  labels: { x: string; y: string; value: string },
  xValues: string[],
  yValues: string[],
): string {
  const x = xValues[params.value[0]] ?? "";
  const y = yValues[params.value[1]] ?? "";
  const v = params.value[2] as number;
  return [
    `<div style="opacity:0.7">${escapeHtml(labels.x)}: ${escapeHtml(x)}</div>`,
    `<div style="opacity:0.7">${escapeHtml(labels.y)}: ${escapeHtml(y)}</div>`,
    `<b>${escapeHtml(labels.value)}: ${escapeHtml(formatChartValue(v))}</b>`,
  ].join("");
}

export function scatterTooltipHtml(
  params: any,
  xLabel: string,
  yLabel: string,
  points: readonly { label: string }[],
): string {
  const p = Array.isArray(params) ? params[0] : params;
  const [x, y] = p.value as [number, number];
  const label = points[p.dataIndex as number]?.label;
  const lines: string[] = [];
  if (label) lines.push(header(label));
  lines.push(
    `${escapeHtml(xLabel)} <b>${escapeHtml(formatChartValue(x))}</b>`,
    `${escapeHtml(yLabel)} <b>${escapeHtml(formatChartValue(y))}</b>`,
  );
  return lines.join("<br/>");
}

export function choroplethTooltipHtml(params: any, valueCol: string): string {
  if (params.data?.value == null) {
    return `${header(params.name)}<span style="opacity:0.5">No data</span>`;
  }
  const d = params.data as { value: number; isoCode: string };
  const code = d.isoCode ? ` (${escapeHtml(d.isoCode)})` : "";
  return `<div style="opacity:0.7">${escapeHtml(params.name)}${code}</div><b>${escapeHtml(formatChartValue(d.value))}</b> ${escapeHtml(valueCol)}`;
}

export function geoBubbleTooltipHtml(params: any, valueCol: string): string {
  if (!params.data) return "";
  const d = params.data as { value: number[]; name: string };
  const [lon, lat, val] = d.value;
  const coords = `<div style="opacity:0.5">${escapeHtml(lat!.toFixed(4))}, ${escapeHtml(lon!.toFixed(4))}</div>`;
  return `${header(d.name)}${coords}<b>${escapeHtml(formatChartValue(val!))}</b> ${escapeHtml(valueCol)}`;
}

export function timeSeriesTooltipHtml(
  params: any,
  formatTime: (d: Date) => string,
): string {
  const items: any[] = Array.isArray(params) ? params : [params];
  if (items.length === 0) return "";
  const lines = items.map(
    (p) => `${dot(p.color as string, 6)}${escapeHtml(p.seriesName)} <b>${escapeHtml(formatChartValue(p.value[1] as number))}</b>`,
  );
  return `${header(formatTime(new Date(items[0].value[0])))}${lines.join("<br/>")}`;
}

export interface HistogramTooltipDeps {
  buckets: HistogramData["buckets"];
  hasGroups: boolean;
  groupKeys: string[];
  colorMap: Map<string, string>;
  copperColor: string;
  hoveredGroup: string | null;
  formatTime: (d: Date) => string;
}

function histogramLine(
  color: string,
  label: string,
  count: number,
  isBold: boolean,
  dimmed: boolean,
): string {
  let style: string;
  if (isBold) style = "font-weight:bold";
  else if (dimmed) style = "opacity:0.5";
  else style = "opacity:0.7";
  const valueStyle = isBold ? "font-weight:bold" : "";
  return `${dot(color, 5)}<span style="${style}">${escapeHtml(label)}</span> <span style="${valueStyle}">${escapeHtml(count.toLocaleString())}</span>`;
}

export function histogramTooltipHtml(params: any, deps: HistogramTooltipDeps): string {
  const items: any[] = Array.isArray(params) ? params : [params];
  if (items.length === 0) return "";
  const bucket = deps.buckets[items[0].dataIndex as number];
  if (!bucket) return "";

  const dimmed = Boolean(deps.hoveredGroup);
  const lines: string[] = [];
  if (deps.hasGroups) {
    const groupSum = Object.values(bucket.groupCounts).reduce((a, b) => a + b, 0);
    const other = bucket.count - groupSum;
    if (other > 0) {
      lines.push(histogramLine(deps.copperColor, "other", other, deps.hoveredGroup === "other", dimmed));
    }
    for (const key of deps.groupKeys.toReversed()) {
      const count = bucket.groupCounts[key];
      if (count && count > 0) {
        const color = resolveColor(deps.colorMap.get(key) ?? deps.copperColor);
        lines.push(histogramLine(color, key, count, deps.hoveredGroup === key, dimmed));
      }
    }
  }

  if (bucket.hasCloudData) {
    lines.push(`<div style="opacity:0.5;font-size:0.85em;margin-top:2px">includes interpolated cloud data</div>`);
  }

  return header(`${bucket.count.toLocaleString()} · ${deps.formatTime(bucket.ts)}`) + lines.join("<br/>");
}
