import type { EChartsOption } from "echarts";
import { cssVar } from "./chartColors";

/**
 * Build common ECharts option defaults that match our design system.
 * Reads CSS variables at call time so it adapts to light/dark mode.
 */
export function buildThemeOption(dark: boolean): EChartsOption {
  const textGhost = dark
    ? cssVar("--color-text-muted")
    : cssVar("--color-light-text-muted");
  // Grid lines use low-opacity white/black — palette-neutral on any dark/light bg.
  const gridLine = dark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.06)";
  const tooltipBg = dark
    ? cssVar("--color-ink-surface")
    : cssVar("--color-light-surface");
  const tooltipBorder = dark
    ? cssVar("--color-ink-border-subtle")
    : cssVar("--color-light-border-subtle");
  const textBright = dark
    ? cssVar("--color-text-bright")
    : cssVar("--color-light-text-bright");

  return {
    animation: true,
    animationDuration: 400,
    animationDurationUpdate: 300,
    animationEasing: "cubicInOut",
    animationEasingUpdate: "cubicInOut",
    textStyle: {
      fontFamily: "'IBM Plex Mono', monospace",
      fontSize: 10,
      color: textGhost,
    },
    grid: {
      containLabel: true,
    },
    xAxis: {
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 10,
        color: textGhost,
      },
      splitLine: { lineStyle: { color: gridLine } },
    },
    yAxis: {
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 10,
        color: textGhost,
      },
      splitLine: { lineStyle: { color: gridLine } },
    },
    tooltip: {
      backgroundColor: tooltipBg,
      borderColor: tooltipBorder,
      textStyle: {
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 12,
        color: textBright,
      },
      padding: [4, 8],
      extraCssText: "border-radius: 4px; box-shadow: 0 2px 8px rgba(0,0,0,0.3);",
    },
  };
}
