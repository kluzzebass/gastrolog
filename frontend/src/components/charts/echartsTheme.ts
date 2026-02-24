import type { EChartsOption } from "echarts";

/** Resolve a CSS variable to its computed value. */
function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

/**
 * Build common ECharts option defaults that match our design system.
 * Reads CSS variables at call time so it adapts to light/dark mode.
 */
export function buildThemeOption(dark: boolean): EChartsOption {
  const textGhost = dark
    ? cssVar("--color-text-ghost") || "rgba(255,255,255,0.35)"
    : cssVar("--color-light-text-ghost") || "rgba(0,0,0,0.35)";
  const gridLine = dark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.06)";
  const tooltipBg = dark
    ? cssVar("--color-ink-surface") || "#1a1a1a"
    : cssVar("--color-light-surface") || "#ffffff";
  const tooltipBorder = dark
    ? cssVar("--color-ink-border-subtle") || "rgba(255,255,255,0.08)"
    : cssVar("--color-light-border-subtle") || "rgba(0,0,0,0.08)";
  const textBright = dark
    ? cssVar("--color-text-bright") || "#e5e5e5"
    : cssVar("--color-light-text-bright") || "#1a1a1a";

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
