import { useState, useEffect, useCallback } from "react";
import { usePreferences, usePutPreferences } from "../api/hooks/usePreferences";
import type { Theme } from "../utils";

export type HighlightMode = "full" | "subtle";

/**
 * Manages the theme setting with localStorage persistence and server sync.
 * Returns current theme, setter, dark flag, and system preference.
 * Also manages syntax highlight mode ("full" | "subtle").
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
  const [systemDark, setSystemDark] = useState(
    () => globalThis.matchMedia("(prefers-color-scheme: dark)").matches,
  );
  const preferences = usePreferences();
  const putPreferences = usePutPreferences();

  // Sync theme and highlight mode from server preferences (in case it changed on another device).
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
      setPrefsLoaded(true);
    }
  }, [preferences.data, prefsLoaded]);

  const setTheme = useCallback(
    (t: Theme) => {
      setThemeLocal(t);
      localStorage.setItem("gastrolog:theme", t);
      putPreferences.mutate({ theme: t, syntaxHighlight: highlightMode });
    },
    [putPreferences, highlightMode],
  );

  const setHighlightMode = useCallback(
    (h: HighlightMode) => {
      setHighlightLocal(h);
      localStorage.setItem("gastrolog:highlight", h);
      putPreferences.mutate({ theme, syntaxHighlight: h });
    },
    [putPreferences, theme],
  );

  useEffect(() => {
    const mq = globalThis.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => setSystemDark(e.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  const dark = theme === "dark" || (theme === "system" && systemDark);

  return { theme, setTheme, dark, highlightMode, setHighlightMode } as const;
}
