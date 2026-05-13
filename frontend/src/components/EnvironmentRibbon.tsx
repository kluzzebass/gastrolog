interface EnvironmentRibbonProps {
  label: string;
  color: string;
}

/**
 * Bottom-left corner ribbon stamped with the deployment name (gastrolog-4vr0l).
 * Fixed position so it sits above page content regardless of scroll. The
 * ribbon is `pointer-events-none` so it never intercepts clicks on the
 * underlying UI (helpful since it overlaps the bottom-left of the viewport
 * where the TanStack devtools button used to live).
 *
 * Color is a deploy-time CLI flag. Text color is auto-picked from the
 * background's perceived brightness (YIQ formula, threshold 0.5) so that
 * white-on-yellow and similar unreadable combos resolve to black-on-yellow
 * automatically.
 */
export function EnvironmentRibbon({ label, color }: Readonly<EnvironmentRibbonProps>) {
  if (!label) return null;
  const textColor = pickContrastingTextColor(color);
  return (
    <div className="fixed bottom-0 left-0 w-72 h-72 overflow-hidden pointer-events-none z-50 select-none">
      <div
        className="absolute -left-14 bottom-9 w-96 text-center font-body font-black text-2xl py-2 tracking-wider uppercase"
        style={{
          transform: "rotate(30deg)",
          color: textColor,
          // Layered background: a linear-gradient with alternating low-alpha
          // white / black stops simulates the highlights and shadows of
          // fabric folds; the solid operator color sits underneath so it
          // shows through. Gradient runs `to right` in the ribbon's local
          // frame, which after rotation runs along the ribbon's length —
          // the direction folds in real fabric would catch light.
          background: `linear-gradient(75deg,
              rgba(255, 255, 255, 0.18) 0%,
              rgba(0, 0, 0, 0.12) 25%,
              rgba(255, 255, 255, 0.18) 50%,
              rgba(0, 0, 0, 0.12) 75%,
              rgba(255, 255, 255, 0.18) 100%), ${color}`,
          // Saturate dims the color; drop-shadow adds a soft dark halo
          // that traces the ribbon's outline so it reads as a piece of
          // fabric wrapped around the viewport corner rather than a flat
          // overlay. Filters are space-separated.
          filter: "saturate(0.7) drop-shadow(0 0 3px rgba(0, 0, 0, 0.5))",
        }}
        title={`Environment: ${label}`}
      >
        <span className="inline-block" style={{ transform: "translateX(-16px)" }}>
          {label}
        </span>
      </div>
    </div>
  );
}

/**
 * pickContrastingTextColor returns "black" or "white" so the ribbon's label
 * stays legible regardless of the operator's color choice. Uses a YIQ
 * perceived-brightness formula at threshold 0.5.
 *
 * Returns "white" on empty / unresolvable inputs so the ribbon still draws
 * something readable on the palette default.
 */
export function pickContrastingTextColor(color: string): "white" | "black" {
  const rgb = parseColor(color);
  if (!rgb) return "white";
  // YIQ perceived brightness; threshold 0.5 is the conventional readability split.
  const brightness = (rgb.r * 299 + rgb.g * 587 + rgb.b * 114) / 1000 / 255;
  return brightness > 0.5 ? "black" : "white";
}

/**
 * parseColor accepts hex (#rgb / #rrggbb), rgb()/rgba(), and a small allowlist
 * of named CSS colors. Returns null on unparseable input.
 *
 * We don't lean on `getComputedStyle` because `happy-dom` (the test runtime)
 * returns CSS values verbatim instead of resolving them to `rgb()`, so a
 * browser-only parse would silently regress in tests. The named-color
 * allowlist covers the ones operators actually use for environment banners;
 * exotic names ("rebeccapurple") fall back to the safe default.
 */
function parseColor(input: string): { r: number; g: number; b: number } | null {
  if (!input) return null;
  const s = input.trim().toLowerCase();

  // Hex: #rgb, #rgba, #rrggbb, #rrggbbaa. Alpha is ignored for brightness.
  if (s.startsWith("#")) {
    const hex = s.slice(1);
    let r: number, g: number, b: number;
    if (hex.length === 3 || hex.length === 4) {
      // Expand shorthand `abc` to `aabbcc` before parsing.
      const expanded = hex.slice(0, 3).split("").map((ch) => ch + ch).join("");
      r = parseInt(expanded.slice(0, 2), 16);
      g = parseInt(expanded.slice(2, 4), 16);
      b = parseInt(expanded.slice(4, 6), 16);
    } else if (hex.length === 6 || hex.length === 8) {
      r = parseInt(hex.slice(0, 2), 16);
      g = parseInt(hex.slice(2, 4), 16);
      b = parseInt(hex.slice(4, 6), 16);
    } else {
      return null;
    }
    if (Number.isNaN(r) || Number.isNaN(g) || Number.isNaN(b)) return null;
    return { r, g, b };
  }

  // rgb() / rgba() — first three numeric channels.
  const rgbMatch = s.match(/^rgba?\(\s*(\d+)\s*[,\s]\s*(\d+)\s*[,\s]\s*(\d+)/);
  if (rgbMatch) {
    return {
      r: Number(rgbMatch[1]),
      g: Number(rgbMatch[2]),
      b: Number(rgbMatch[3]),
    };
  }

  return NAMED_COLORS[s] ?? null;
}

// Static allowlist of CSS color names that operators commonly reach for when
// labeling environments. Browsers know hundreds more; extend this when an
// operator hits a name that resolves to fallback white text.
const NAMED_COLORS: Record<string, { r: number; g: number; b: number }> = {
  // primaries + dark
  red: { r: 255, g: 0, b: 0 },
  green: { r: 0, g: 128, b: 0 },
  blue: { r: 0, g: 0, b: 255 },
  black: { r: 0, g: 0, b: 0 },
  navy: { r: 0, g: 0, b: 128 },
  darkred: { r: 139, g: 0, b: 0 },
  darkgreen: { r: 0, g: 100, b: 0 },
  darkblue: { r: 0, g: 0, b: 139 },
  // bright + light
  white: { r: 255, g: 255, b: 255 },
  yellow: { r: 255, g: 255, b: 0 },
  lime: { r: 0, g: 255, b: 0 },
  cyan: { r: 0, g: 255, b: 255 },
  aqua: { r: 0, g: 255, b: 255 },
  magenta: { r: 255, g: 0, b: 255 },
  fuchsia: { r: 255, g: 0, b: 255 },
  lightyellow: { r: 255, g: 255, b: 224 },
  // accents commonly picked for ribbons
  orange: { r: 255, g: 165, b: 0 },
  darkorange: { r: 255, g: 140, b: 0 },
  gold: { r: 255, g: 215, b: 0 },
  limegreen: { r: 50, g: 205, b: 50 },
  dodgerblue: { r: 30, g: 144, b: 255 },
  deepskyblue: { r: 0, g: 191, b: 255 },
  hotpink: { r: 255, g: 105, b: 180 },
  deeppink: { r: 255, g: 20, b: 147 },
  purple: { r: 128, g: 0, b: 128 },
  indigo: { r: 75, g: 0, b: 130 },
  teal: { r: 0, g: 128, b: 128 },
};
