import { useEffect, useRef, useState } from "react";
import type mermaidAPI from "mermaid";
import { useThemeClass } from "../hooks/useThemeClass";

// Lazy singleton — loaded on first render, not at import time.
let mermaidModule: typeof mermaidAPI | null = null;
async function getMermaid() {
  if (!mermaidModule) {
    const m = await import("mermaid");
    mermaidModule = m.default;
  }
  return mermaidModule;
}

/** Resolve a CSS variable to its computed value. */
function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

/**
 * Build Mermaid theme variables from CSS design tokens so diagrams adapt
 * to whichever palette is active (Observatory, Nord, Solarized, etc.).
 */
function buildMermaidVars(dark: boolean) {
  if (dark) {
    const raised = cssVar("--color-ink-raised");
    const surface = cssVar("--color-ink-surface");
    const hover = cssVar("--color-ink-hover");
    const border = cssVar("--color-ink-border");
    const borderSubtle = cssVar("--color-ink-border-subtle");
    const copperDim = cssVar("--color-copper-dim");
    const textBright = cssVar("--color-text-bright");
    const textNormal = cssVar("--color-text-normal");
    return {
      background: raised,
      primaryColor: surface,
      primaryBorderColor: copperDim,
      primaryTextColor: textBright,
      secondaryColor: hover,
      secondaryBorderColor: border,
      secondaryTextColor: textNormal,
      tertiaryColor: borderSubtle,
      tertiaryBorderColor: border,
      tertiaryTextColor: textNormal,
      lineColor: copperDim,
      textColor: textNormal,
      mainBkg: surface,
      nodeBorder: copperDim,
      clusterBkg: raised,
      clusterBorder: border,
      titleColor: textBright,
      edgeLabelBackground: raised,
      nodeTextColor: textBright,
    };
  }
  const raised = cssVar("--color-light-raised");
  const surface = cssVar("--color-light-surface");
  const hover = cssVar("--color-light-hover");
  const border = cssVar("--color-light-border");
  const borderSubtle = cssVar("--color-light-border-subtle");
  const copperDim = cssVar("--color-copper-dim");
  const textBright = cssVar("--color-light-text-bright");
  const textNormal = cssVar("--color-light-text-normal");
  return {
    background: raised,
    primaryColor: surface,
    primaryBorderColor: copperDim,
    primaryTextColor: textBright,
    secondaryColor: hover,
    secondaryBorderColor: border,
    secondaryTextColor: textNormal,
    tertiaryColor: borderSubtle,
    tertiaryBorderColor: border,
    tertiaryTextColor: textNormal,
    lineColor: copperDim,
    textColor: textNormal,
    mainBkg: surface,
    nodeBorder: copperDim,
    clusterBkg: raised,
    clusterBorder: border,
    titleColor: textBright,
    edgeLabelBackground: raised,
    nodeTextColor: textBright,
  };
}

function initMermaid(m: typeof mermaidAPI, dark: boolean) {
  m.initialize({
    startOnLoad: false,
    theme: "base",
    themeVariables: buildMermaidVars(dark),
    fontFamily: '"Libre Franklin", sans-serif',
  });
}

// Module-level SVG cache keyed by chart+dark. When react-markdown remounts
// MermaidDiagram (e.g. because parent re-renders recreate the components
// object), the cached SVG is shown immediately instead of re-running the
// async mermaid render, which would cause visible flicker.
const svgCache = new Map<string, string>();

function cacheKey(chart: string, dark: boolean): string {
  return `${dark ? "d" : "l"}:${chart}`;
}

interface MermaidDiagramProps {
  chart: string;
  dark: boolean;
}

export function MermaidDiagram({ chart, dark }: Readonly<MermaidDiagramProps>) {
  const c = useThemeClass(dark);
  const ref = useRef<HTMLDivElement>(null);
  const key = cacheKey(chart, dark);
  const cached = svgCache.get(key);
  const [svg, setSvg] = useState(cached ?? "");
  const [error, setError] = useState("");

  useEffect(() => {
    // Already have the SVG (from cache or a previous render) — nothing to do.
    if (svgCache.has(key)) return;

    const id = `mermaid-${Math.random().toString(36).slice(2, 11)}`;
    let cancelled = false;

    getMermaid()
      .then((m) => {
        if (cancelled) return;
        initMermaid(m, dark);
        return m.render(id, chart);
      })
      .then((result) => {
        if (cancelled || !result) return;
        svgCache.set(key, result.svg);
        setSvg(result.svg);
        setError("");
      })
      .catch(() => {
        if (cancelled) return;
        setError("Failed to render diagram");
      });

    return () => { cancelled = true; };
  }, [key, chart, dark]);

  if (error) {
    return (
      <div
        className={`mb-3 p-3 rounded text-[0.85em] ${c("bg-ink-surface text-text-muted", "bg-light-hover text-light-text-muted")}`}
      >
        {error}
      </div>
    );
  }

  return (
    <div
      ref={ref}
      className="mb-3 overflow-x-auto app-scroll [&_svg]:max-w-full"
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
}
