import { useEffect, useRef, useState } from "react";
import mermaid from "mermaid";
import { useThemeClass } from "../hooks/useThemeClass";

const darkVars = {
  background: "#141820",
  primaryColor: "#1a1f2a",
  primaryBorderColor: "#a06b44",
  primaryTextColor: "#e8e4df",
  secondaryColor: "#1f2536",
  secondaryBorderColor: "#2a3040",
  secondaryTextColor: "#b8b2aa",
  tertiaryColor: "#222838",
  tertiaryBorderColor: "#2a3040",
  tertiaryTextColor: "#b8b2aa",
  lineColor: "#a06b44",
  textColor: "#b8b2aa",
  mainBkg: "#1a1f2a",
  nodeBorder: "#a06b44",
  clusterBkg: "#141820",
  clusterBorder: "#2a3040",
  titleColor: "#e8e4df",
  edgeLabelBackground: "#141820",
  nodeTextColor: "#e8e4df",
};

const lightVars = {
  background: "#faf8f4",
  primaryColor: "#ffffff",
  primaryBorderColor: "#a06b44",
  primaryTextColor: "#1a1610",
  secondaryColor: "#ede8e0",
  secondaryBorderColor: "#d8d0c4",
  secondaryTextColor: "#3a3630",
  tertiaryColor: "#f4f0ea",
  tertiaryBorderColor: "#d8d0c4",
  tertiaryTextColor: "#3a3630",
  lineColor: "#a06b44",
  textColor: "#3a3630",
  mainBkg: "#ffffff",
  nodeBorder: "#a06b44",
  clusterBkg: "#faf8f4",
  clusterBorder: "#d8d0c4",
  titleColor: "#1a1610",
  edgeLabelBackground: "#faf8f4",
  nodeTextColor: "#1a1610",
};

function initMermaid(dark: boolean) {
  mermaid.initialize({
    startOnLoad: false,
    theme: "base",
    themeVariables: dark ? darkVars : lightVars,
    fontFamily: '"Libre Franklin", sans-serif',
  });
}

interface MermaidDiagramProps {
  chart: string;
  dark: boolean;
}

export function MermaidDiagram({ chart, dark }: Readonly<MermaidDiagramProps>) {
  const c = useThemeClass(dark);
  const ref = useRef<HTMLDivElement>(null);
  const [result, setResult] = useState<{ svg: string; error: string }>({ svg: "", error: "" });

  useEffect(() => {
    initMermaid(dark);
    const id = `mermaid-${Math.random().toString(36).slice(2, 11)}`;
    mermaid
      .render(id, chart)
      .then(({ svg }) => setResult({ svg, error: "" }))
      .catch(() => setResult({ svg: "", error: "Failed to render diagram" }));
  }, [chart, dark]);

  if (result.error) {
    return (
      <div
        className={`mb-3 p-3 rounded text-[0.85em] ${c("bg-ink-surface text-text-ghost", "bg-light-hover text-light-text-ghost")}`}
      >
        {result.error}
      </div>
    );
  }

  return (
    <div
      ref={ref}
      className="mb-3 overflow-x-auto app-scroll [&_svg]:max-w-full"
      dangerouslySetInnerHTML={{ __html: result.svg }}
    />
  );
}
