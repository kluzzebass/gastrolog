import { useState, useEffect, useCallback } from "react";
import { usePreferences, usePutPreferences } from "../api/hooks/usePreferences";
import type { Theme, Palette } from "../utils";

export type HighlightMode = "full" | "subtle";

const PALETTES: Palette[] = ["observatory", "nord", "solarized", "dracula", "catppuccin", "gruvbox", "tokyonight", "rosepine", "everforest", "synthwave"];

function isPalette(v: string): v is Palette {
  return (PALETTES as string[]).includes(v);
}

/**
 * Manages the theme setting with localStorage persistence and server sync.
 * Returns current theme, setter, dark flag, and system preference.
 * Also manages syntax highlight mode ("full" | "subtle") and palette.
 */
export function useThemeSync() {
  const [theme, setThemeLocal] = useState<Theme>(() => {
    const cached = localStorage.getItem("gastrolog:theme");
    if (cached === "light" || cached === "dark" || cached === "system")
      return cached;
    return "system";
  });
  const [highlightMode, setHighlightLocal] = useState<HighlightMode>(() => {
    const cached = localStorage.getItem("gastrolog:highlight");
    if (cached === "full" || cached === "subtle") return cached;
    return "full";
  });
  const [palette, setPaletteLocal] = useState<Palette>(() => {
    const cached = localStorage.getItem("gastrolog:palette");
    if (cached && isPalette(cached)) return cached;
    return "observatory";
  });
  const [systemDark, setSystemDark] = useState(
    () => globalThis.matchMedia("(prefers-color-scheme: dark)").matches,
  );
  const preferences = usePreferences();
  const putPreferences = usePutPreferences();

  // Sync theme, highlight mode, and palette from server preferences (in case it changed on another device).
  const [prefsLoaded, setPrefsLoaded] = useState(false);
  useEffect(() => {
    if (preferences.data && !prefsLoaded) {
      const t = preferences.data.theme;
      if (t === "light" || t === "dark" || t === "system") {
        setThemeLocal(t);
        localStorage.setItem("gastrolog:theme", t);
      }
      const h = preferences.data.syntaxHighlight;
      if (h === "full" || h === "subtle") {
        setHighlightLocal(h);
        localStorage.setItem("gastrolog:highlight", h);
      }
      const p = preferences.data.palette;
      if (p && isPalette(p)) {
        setPaletteLocal(p);
        localStorage.setItem("gastrolog:palette", p);
      }
      setPrefsLoaded(true);
    }
  }, [preferences.data, prefsLoaded]);

  const setTheme = useCallback(
    (t: Theme) => {
      setThemeLocal(t);
      localStorage.setItem("gastrolog:theme", t);
      putPreferences.mutate({ theme: t, syntaxHighlight: highlightMode, palette });
    },
    [putPreferences, highlightMode, palette],
  );

  const setHighlightMode = useCallback(
    (h: HighlightMode) => {
      setHighlightLocal(h);
      localStorage.setItem("gastrolog:highlight", h);
      putPreferences.mutate({ theme, syntaxHighlight: h, palette });
    },
    [putPreferences, theme, palette],
  );

  const setPalette = useCallback(
    (p: Palette) => {
      setPaletteLocal(p);
      localStorage.setItem("gastrolog:palette", p);
      putPreferences.mutate({ theme, syntaxHighlight: highlightMode, palette: p });
    },
    [putPreferences, theme, highlightMode],
  );

  // Apply palette class to <html> so it takes effect immediately regardless
  // of which hook instance calls setPalette.
  useEffect(() => {
    const root = document.documentElement;
    for (const p of PALETTES) {
      if (p !== "observatory") root.classList.remove(`theme-${p}`);
    }
    if (palette !== "observatory") {
      root.classList.add(`theme-${palette}`);
    }
  }, [palette]);

  useEffect(() => {
    const mq = globalThis.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => setSystemDark(e.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  const dark = theme === "dark" || (theme === "system" && systemDark);

  return { theme, setTheme, dark, highlightMode, setHighlightMode, palette, setPalette } as const;
}
