import { useRef } from "react";
import ReactEChartsCore from "echarts-for-react/esm/core";
import { feature } from "topojson-client";
import type { Topology } from "topojson-specification";
import worldTopo from "world-atlas/countries-110m.json";
import { echarts } from "./echartsSetup";
import { buildThemeOption } from "./echartsTheme";
import { resolveColor, formatChartValue } from "./chartColors";
import { isoToMapName } from "./countryMapping";
import type { EChartsOption } from "echarts";

/**
 * Clip a ring at the antimeridian (±180° longitude).
 *
 * Walks each edge; when consecutive vertices jump across ±180°, interpolates
 * the latitude at the crossing and inserts boundary points to close each side.
 * Returns one or more closed rings, each staying on one side of the dateline.
 */
function clipRingAtAntimeridian(ring: number[][]): number[][][] {
  const segments: number[][][] = [];
  let current: number[][] = [];

  for (let i = 0; i < ring.length; i++) {
    const p = ring[i]!;
    if (current.length === 0) {
      current.push(p);
      continue;
    }
    const prev = current[current.length - 1]!;

    // Detect antimeridian crossing: one lon near +180, next near -180 (or vice versa).
    if (crossesEdge(prev[0]!, p[0]!)) {
      // Interpolate latitude at the crossing.
      const lat = interpolateLat(prev, p);
      // Close current segment at the boundary.
      const boundaryLon = prev[0]! > 0 ? 180 : -180;
      current.push([boundaryLon, lat]);
      segments.push(current);
      // Start new segment on the other side.
      current = [[-boundaryLon, lat], p];
    } else {
      current.push(p);
    }
  }

  if (current.length > 0) segments.push(current);

  // If the ring was split, the first and last segment may be on the same side —
  // merge them to form a closed ring.
  if (segments.length > 1) {
    const first = segments[0]!;
    const last = segments[segments.length - 1]!;
    const firstSide = first[0]![0]! > 0;
    const lastSide = last[0]![0]! > 0;
    if (firstSide === lastSide) {
      segments[0] = [...last, ...first];
      segments.pop();
    }
  }

  // Close each segment as a ring and filter out degenerate ones.
  return segments
    .map((seg) => {
      if (seg.length < 3) return null;
      // Ensure ring closure.
      const f = seg[0]!;
      const l = seg[seg.length - 1]!;
      if (f[0] !== l[0] || f[1] !== l[1]) seg.push([f[0]!, f[1]!]);
      return seg;
    })
    .filter((s): s is number[][] => s !== null && s.length >= 4);
}

/** True if the edge from lon1 to lon2 crosses the antimeridian. */
function crossesEdge(lon1: number, lon2: number): boolean {
  return Math.abs(lon1 - lon2) > 180;
}

/** Interpolate latitude where an edge from p1 to p2 crosses ±180° longitude. */
function interpolateLat(p1: number[], p2: number[]): number {
  let [lon1] = p1;
  const [, lat1] = p1;
  let [lon2] = p2;
  const [, lat2] = p2;
  // Normalize so we interpolate across the short arc.
  if (lon1! > 0 && lon2! < 0) lon2! += 360;
  else if (lon1! < 0 && lon2! > 0) lon1! += 360;
  const t = (180 - Math.min(lon1!, lon2!)) / Math.abs(lon2! - lon1!);
  return lat1! + t * (lat2! - lat1!);
}

function ringCrossesAntimeridian(ring: number[][]): boolean {
  for (let i = 1; i < ring.length; i++) {
    if (crossesEdge(ring[i - 1]![0]!, ring[i]![0]!)) return true;
  }
  return false;
}

/**
 * Fix GeoJSON features whose polygons cross the antimeridian.
 * Also removes Antarctica which isn't useful for a choropleth.
 */
function fixAntimeridian(geojson: any): any {
  const features = geojson.features.map((f: any) => {
    if (f.id === "010") return null; // Antarctica

    const fixPoly = (poly: number[][][]): number[][][][] => {
      if (!ringCrossesAntimeridian(poly[0]!)) return [poly];
      // Only clip the outer ring; holes in antimeridian-crossing polys are rare at 110m.
      return clipRingAtAntimeridian(poly[0]!).map((ring) => [ring]);
    };

    if (f.geometry.type === "Polygon") {
      const parts = fixPoly(f.geometry.coordinates);
      if (parts.length === 1) return f;
      return { ...f, geometry: { type: "MultiPolygon", coordinates: parts } };
    }

    if (f.geometry.type === "MultiPolygon") {
      const allParts: number[][][][] = [];
      let changed = false;
      for (const poly of f.geometry.coordinates) {
        const parts = fixPoly(poly);
        if (parts.length !== 1 || parts[0] !== poly) changed = true;
        allParts.push(...parts);
      }
      if (changed) {
        return { ...f, geometry: { type: "MultiPolygon", coordinates: allParts } };
      }
    }

    return f;
  });

  return { ...geojson, features: features.filter(Boolean) };
}

// Register the world map once at module load (only runs when this chunk is imported).
const worldGeo = feature(worldTopo as unknown as Topology, (worldTopo as any).objects.countries);
echarts.registerMap("world", fixAntimeridian(worldGeo));

interface WorldMapChartProps {
  columns: string[];
  rows: string[][];
  dark: boolean;
  mode: "choropleth" | "scatter";
}

interface ChoroplethDatum {
  name: string;
  value: number;
  isoCode: string;
}

// Module-level roam state — survives component remounts caused by parent re-renders.
let savedCenter: number[] | undefined;
let savedZoom: number | undefined;

function buildChoroplethOption(
  columns: string[],
  rows: string[][],
  theme: EChartsOption,
  colors: { copper: string; border: string; empty: string; textGhost: string },
): EChartsOption {
  const valueColIdx = columns.length - 1;
  const data: ChoroplethDatum[] = [];
  let maxVal = 0;

  for (const row of rows) {
    const code = columns.length > 2
      ? row.slice(0, columns.length - 1).join(" / ")
      : (row[0] ?? "");
    if (code === "") continue;
    const value = Number(row[valueColIdx]) || 0;
    const name = isoToMapName(code) ?? code;
    data.push({ name, value, isoCode: code });
    if (value > maxVal) maxVal = value;
  }

  return {
    ...theme,
    backgroundColor: "transparent",
    tooltip: {
      ...theme.tooltip as object,
      trigger: "item",
      formatter: (params: any) => {
        if (!params.data || params.data.value == null) {
          return `<div style="opacity:0.7">${params.name}</div><span style="opacity:0.5">No data</span>`;
        }
        const d = params.data as ChoroplethDatum;
        const code = d.isoCode ? ` (${d.isoCode})` : "";
        return `<div style="opacity:0.7">${params.name}${code}</div><b>${formatChartValue(d.value)}</b> ${columns[valueColIdx]}`;
      },
    },
    visualMap: [
      {
        type: "continuous",
        min: 0,
        max: maxVal || 1,
        text: [formatChartValue(maxVal || 1), "0"],
        inRange: { color: [colors.empty, colors.copper] },
        textStyle: {
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: 10,
          color: colors.textGhost,
        },
        left: 16,
        bottom: 16,
        itemWidth: 12,
        itemHeight: 80,
        calculable: false,
      },
    ],
    series: [
      {
        type: "map",
        map: "world",
        roam: true,
        ...(savedCenter ? { center: savedCenter } : {}),
        ...(savedZoom ? { zoom: savedZoom } : {}),
        scaleLimit: { min: 1, max: 8 },
        itemStyle: {
          areaColor: colors.empty,
          borderColor: colors.border,
          borderWidth: 0.5,
        },
        emphasis: {
          itemStyle: {
            areaColor: colors.copper,
            borderColor: colors.copper,
            borderWidth: 1,
          },
          label: { show: false },
        },
        select: { disabled: true },
        data,
      },
    ],
  };
}

function buildScatterOption(
  columns: string[],
  rows: string[][],
  latIdx: number,
  lonIdx: number,
  theme: EChartsOption,
  colors: { copper: string; border: string; empty: string; textGhost: string },
): EChartsOption {
  const valueColIdx = columns.length - 1;
  const valueCol = columns[valueColIdx]!;

  // Build label from any extra group-by columns (not lat, lon, or value).
  const extraCols = columns
    .map((c, i) => ({ c, i }))
    .filter(({ i }) => i !== latIdx && i !== lonIdx && i !== valueColIdx);

  const data: { value: number[]; name: string }[] = [];
  let maxVal = 0;

  for (const row of rows) {
    const lat = Number(row[latIdx]);
    const lon = Number(row[lonIdx]);
    const value = Number(row[valueColIdx]) || 0;
    if (isNaN(lat) || isNaN(lon)) continue;
    if (lat === 0 && lon === 0) continue; // GeoIP lookup miss, not a real location
    const name = extraCols.length > 0
      ? extraCols.map(({ i }) => row[i]).join(" / ")
      : `${lat.toFixed(2)}, ${lon.toFixed(2)}`;
    data.push({ value: [lon, lat, value], name });
    if (value > maxVal) maxVal = value;
  }

  // Scale bubble sizes: sqrt scale, min 4px, max 24px.
  const sizeScale = maxVal > 0 ? 20 / Math.sqrt(maxVal) : 1;

  return {
    ...theme,
    backgroundColor: "transparent",
    geo: {
      map: "world",
      roam: true,
      ...(savedCenter ? { center: savedCenter } : {}),
      ...(savedZoom ? { zoom: savedZoom } : {}),
      scaleLimit: { min: 1, max: 8 },
      itemStyle: {
        areaColor: colors.empty,
        borderColor: colors.border,
        borderWidth: 0.5,
      },
      emphasis: {
        itemStyle: {
          areaColor: colors.empty,
          borderColor: colors.border,
        },
        label: { show: false },
      },
    },
    tooltip: {
      ...theme.tooltip as object,
      trigger: "item",
      formatter: (params: any) => {
        if (!params.data) return "";
        const d = params.data as { value: number[]; name: string };
        const [lon, lat, val] = d.value;
        return `<div style="opacity:0.7">${d.name}</div><div style="opacity:0.5">${lat!.toFixed(4)}, ${lon!.toFixed(4)}</div><b>${formatChartValue(val!)}</b> ${valueCol}`;
      },
    },
    visualMap: [
      {
        type: "continuous",
        min: 0,
        max: maxVal || 1,
        dimension: 2,
        text: [formatChartValue(maxVal || 1), "0"],
        inRange: { color: [colors.copper + "40", colors.copper] },
        textStyle: {
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: 10,
          color: colors.textGhost,
        },
        left: 16,
        bottom: 16,
        itemWidth: 12,
        itemHeight: 80,
        calculable: false,
      },
    ],
    series: [
      {
        type: "scatter",
        coordinateSystem: "geo",
        data,
        symbolSize: (val: number[]) => Math.max(4, Math.sqrt(val[2]!) * sizeScale),
        itemStyle: { borderColor: colors.copper, borderWidth: 0.5 },
        emphasis: {
          itemStyle: { borderWidth: 2 },
          scale: 1.5,
        },
      },
    ],
  };
}

export function WorldMapChart({
  columns,
  rows,
  dark,
  mode,
}: Readonly<WorldMapChartProps>) {
  const chartRef = useRef<ReactEChartsCore>(null);
  const theme = buildThemeOption(dark);

  const copperBase = resolveColor("var(--color-copper)") || "#c87941";
  const bgColor = dark ? "rgba(255,255,255,0.04)" : "rgba(0,0,0,0.05)";
  const colors = {
    copper: copperBase,
    border: dark ? "rgba(255,255,255,0.20)" : "rgba(0,0,0,0.30)",
    empty: dark ? "rgba(255,255,255,0.15)" : "rgba(0,0,0,0.17)",
    textGhost: dark ? "rgba(255,255,255,0.35)" : "rgba(0,0,0,0.35)",
  };

  const isScatter = mode === "scatter";
  // For scatter, the stats group-by clause puts lat/lon as the first two columns.
  const option = isScatter
    ? buildScatterOption(columns, rows, 0, 1, theme, colors)
    : buildChoroplethOption(columns, rows, theme, colors);

  const onEvents = {
    geoRoam: () => {
      const instance = chartRef.current?.getEchartsInstance();
      if (!instance) return;
      const opt = instance.getOption() as any;
      // Scatter mode uses geo component, choropleth uses map series.
      if (isScatter) {
        const geo = opt.geo?.[0] ?? opt.geo;
        if (geo) {
          savedCenter = geo.center;
          savedZoom = geo.zoom;
        }
      } else {
        const series = opt.series?.[0];
        if (series) {
          savedCenter = series.center;
          savedZoom = series.zoom;
        }
      }
    },
  };

  return (
    <div style={{ background: bgColor, borderRadius: 6 }}>
      <ReactEChartsCore
        ref={chartRef}
        echarts={echarts}
        option={option}
        style={{ height: 420, width: "100%" }}
        lazyUpdate
        onEvents={onEvents}
      />
    </div>
  );
}
