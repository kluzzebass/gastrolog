import { describe, test, expect } from "bun:test";
import {
  barTooltipHtml,
  donutTooltipHtml,
  heatmapTooltipHtml,
  scatterTooltipHtml,
  choroplethTooltipHtml,
  geoBubbleTooltipHtml,
  timeSeriesTooltipHtml,
  histogramTooltipHtml,
} from "./chartTooltips";
import type { HistogramTooltipDeps } from "./chartTooltips";
import type { HistogramData } from "../../utils/histogramData";

/** What an attacker puts in a log attribute to reach the operator's browser. */
const HOSTILE = `<img src=x onerror=alert(1)>`;

/**
 * A payload made of quotes rather than angle brackets. No builder interpolates
 * a log-derived value into an attribute today, so this cannot execute — it is
 * here so that dropping quote escaping fails a test instead of passing one, and
 * so the next builder that does reach attribute position inherits a helper
 * already known to close it.
 */
const QUOTED_HOSTILE = `" onmouseover="alert(1)`;

/**
 * Render tooltip HTML the way ECharts does and report what the browser made
 * of it: the visible text, and whether any element was constructed.
 */
function render(html: string): { text: string; elements: number } {
  const host = document.createElement("div");
  host.innerHTML = html;
  return { text: host.textContent, elements: host.querySelectorAll("*").length };
}

/** A UTC-only time label so the assertions do not depend on the host timezone. */
const utcYear = (d: Date) => `T${d.getUTCFullYear()}`;

function histogramBucket(groupCounts: Record<string, number>): HistogramData["buckets"][number] {
  return {
    ts: new Date(Date.UTC(2026, 0, 1)),
    count: Object.values(groupCounts).reduce((a, b) => a + b, 0),
    groupCounts,
    hasCloudData: false,
    cloudCount: 0,
  };
}

function deps(groupCounts: Record<string, number>): HistogramTooltipDeps {
  return {
    buckets: [histogramBucket(groupCounts)],
    hasGroups: true,
    groupKeys: Object.keys(groupCounts),
    colorMap: new Map(Object.keys(groupCounts).map((k) => [k, "#c97"])),
    copperColor: "#c97",
    hoveredGroup: null,
    formatTime: () => "00:00",
  };
}

/** Assert the hostile string reached the operator as text, not as markup. */
function expectInert(html: string) {
  const host = document.createElement("div");
  host.innerHTML = html;
  expect(host.querySelector("img")).toBeNull();
  expect(host.querySelector("[onerror]")).toBeNull();
  expect(html).not.toContain("<img");
  expect(html).toContain("&lt;img src=x onerror=alert(1)&gt;");
  // The operator still sees the literal value.
  expect(host.textContent).toContain(HOSTILE);
}

/** Assert quotes were escaped, so the value cannot close an attribute. */
function expectQuotesClosed(html: string) {
  const host = document.createElement("div");
  host.innerHTML = html;
  expect(html).toContain("&quot;");
  expect(html).not.toContain(`"alert(1)`);
  for (const el of host.querySelectorAll("*")) {
    expect(el.hasAttribute("onmouseover")).toBe(false);
  }
  expect(host.textContent).toContain(QUOTED_HOSTILE);
}

describe("barTooltipHtml", () => {
  test("renders a hostile category label inert", () => {
    const html = barTooltipHtml({ name: HOSTILE, color: "#c97", value: 12 }, ["host", "count"]);
    expectInert(html);
  });

  test("a hostile column header cannot break out either", () => {
    const html = barTooltipHtml({ name: "web-1", color: "#c97", value: 12 }, ["host", HOSTILE]);
    expectInert(html);
  });

  test("a quote-bearing category label cannot close an attribute", () => {
    const html = barTooltipHtml({ name: QUOTED_HOSTILE, color: "#c97", value: 12 }, ["host", "count"]);
    expectQuotesClosed(html);
  });

  test("benign values keep their displayed text and markup", () => {
    const { text, elements } = render(
      barTooltipHtml({ name: "web-1", color: "#c97", value: 1200 }, ["host", "count"]),
    );
    expect(text).toBe("web-1count 1.2K");
    // Header div, color dot, and value <b> — the tooltip is not flattened.
    expect(elements).toBe(3);
  });

  test("axis-trigger params arrive as an array", () => {
    const html = barTooltipHtml([{ name: HOSTILE, color: "#c97", value: 3 }], ["host", "count"]);
    expectInert(html);
  });
});

describe("donutTooltipHtml", () => {
  test("renders a hostile slice name inert", () => {
    expectInert(donutTooltipHtml({ name: HOSTILE, color: "#c97", value: 5 }, ["svc", "count"], 10));
  });

  test("benign values keep their text and percentage", () => {
    const { text } = render(donutTooltipHtml({ name: "auth", color: "#c97", value: 5 }, ["svc", "count"], 10));
    expect(text).toBe("authcount 5 (50.0%)");
  });
});

describe("heatmapTooltipHtml", () => {
  test("renders hostile axis values inert", () => {
    const html = heatmapTooltipHtml(
      { value: [0, 0, 7] },
      { x: "hour", y: "host", value: "count" },
      [HOSTILE],
      ["web-1"],
    );
    expectInert(html);
  });

  test("renders a hostile axis label inert", () => {
    const html = heatmapTooltipHtml(
      { value: [0, 0, 7] },
      { x: HOSTILE, y: "host", value: "count" },
      ["03"],
      ["web-1"],
    );
    expectInert(html);
  });

  test("benign values keep their text", () => {
    const { text } = render(
      heatmapTooltipHtml({ value: [0, 0, 7] }, { x: "hour", y: "host", value: "count" }, ["03"], ["web-1"]),
    );
    expect(text).toBe("hour: 03host: web-1count: 7");
  });
});

describe("scatterTooltipHtml", () => {
  test("renders a hostile point label inert", () => {
    expectInert(scatterTooltipHtml({ value: [1, 2], dataIndex: 0 }, "latency", "count", [{ label: HOSTILE }]));
  });

  test("renders a hostile axis name inert", () => {
    expectInert(scatterTooltipHtml({ value: [1, 2], dataIndex: 0 }, HOSTILE, "count", [{ label: "web-1" }]));
  });

  test("benign values keep their text", () => {
    const { text } = render(
      scatterTooltipHtml({ value: [1, 2], dataIndex: 0 }, "latency", "count", [{ label: "web-1" }]),
    );
    expect(text).toBe("web-1latency 1count 2");
  });
});

describe("choroplethTooltipHtml", () => {
  test("renders a hostile country name inert", () => {
    expectInert(choroplethTooltipHtml({ name: HOSTILE, data: { value: 3, isoCode: "NO" } }, "count"));
  });

  test("renders a hostile ISO code inert", () => {
    expectInert(choroplethTooltipHtml({ name: "Norway", data: { value: 3, isoCode: HOSTILE } }, "count"));
  });

  test("the no-data branch escapes the region name too", () => {
    expectInert(choroplethTooltipHtml({ name: HOSTILE, data: undefined }, "count"));
  });

  test("benign values keep their text", () => {
    const { text } = render(choroplethTooltipHtml({ name: "Norway", data: { value: 3, isoCode: "NO" } }, "count"));
    expect(text).toBe("Norway (NO)3 count");
  });
});

describe("geoBubbleTooltipHtml", () => {
  test("renders a hostile point name inert", () => {
    expectInert(geoBubbleTooltipHtml({ data: { value: [10, 60, 4], name: HOSTILE } }, "count"));
  });

  test("benign values keep their coordinates and text", () => {
    const { text } = render(geoBubbleTooltipHtml({ data: { value: [10.5, 59.9, 4], name: "oslo" } }, "count"));
    expect(text).toBe("oslo59.9000, 10.50004 count");
  });
});

describe("timeSeriesTooltipHtml", () => {
  test("renders a hostile series name inert", () => {
    const html = timeSeriesTooltipHtml(
      [{ value: [Date.UTC(2026, 0, 1), 9], seriesName: HOSTILE, color: "#c97" }],
      utcYear,
    );
    expectInert(html);
  });

  test("every series line is escaped, not just the first", () => {
    const html = timeSeriesTooltipHtml(
      [
        { value: [Date.UTC(2026, 0, 1), 9], seriesName: "ok", color: "#c97" },
        { value: [Date.UTC(2026, 0, 1), 2], seriesName: HOSTILE, color: "#c97" },
      ],
      utcYear,
    );
    expectInert(html);
  });

  test("benign values keep their text", () => {
    const { text } = render(
      timeSeriesTooltipHtml([{ value: [Date.UTC(2026, 0, 1), 9], seriesName: "ok", color: "#c97" }], utcYear),
    );
    expect(text).toBe("T2026ok 9");
  });
});

describe("histogramTooltipHtml", () => {
  test("renders a hostile group key inert", () => {
    expectInert(histogramTooltipHtml([{ dataIndex: 0 }], deps({ [HOSTILE]: 4 })));
  });

  test("a quote-bearing group key cannot close the style attribute it sits beside", () => {
    expectQuotesClosed(histogramTooltipHtml([{ dataIndex: 0 }], deps({ [QUOTED_HOSTILE]: 4 })));
  });

  test("a hovered hostile group is still escaped in its bold line", () => {
    const d = { ...deps({ [HOSTILE]: 4 }), hoveredGroup: HOSTILE };
    expectInert(histogramTooltipHtml([{ dataIndex: 0 }], d));
  });

  test("benign groups keep their text", () => {
    const { text } = render(histogramTooltipHtml([{ dataIndex: 0 }], deps({ error: 4 })));
    expect(text).toBe("4 · 00:00error 4");
  });

  test("an unknown bucket index yields nothing", () => {
    expect(histogramTooltipHtml([{ dataIndex: 99 }], deps({ error: 4 }))).toBe("");
  });
});
