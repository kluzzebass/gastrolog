import { useState, useEffect, useCallback } from "react";
import { usePreferences, usePutPreferences } from "../api/hooks/usePreferences";
import type { Theme } from "../utils";

/**
 * Manages the theme setting with localStorage persistence and server sync.
 * Returns current theme, setter, dark flag, and system preference.
 */
export function useThemeSync() {
  const [theme, setThemeLocal] = useState<Theme>(() => {
    const cached = localStorage.getItem("gastrolog:theme");
    if (cached === "light" || cached === "dark" || cached === "system")
      return cached;
    return "system";
  });
  const [systemDark, setSystemDark] = useState(
    () => window.matchMedia("(prefers-color-scheme: dark)").matches,
  );
  const preferences = usePreferences();
  const putPreferences = usePutPreferences();

  // Sync theme from server preferences (in case it changed on another device).
  const [prefsLoaded, setPrefsLoaded] = useState(false);
  useEffect(() => {
    if (preferences.data && !prefsLoaded) {
      const t = preferences.data.theme;
      if (t === "light" || t === "dark" || t === "system") {
        setThemeLocal(t);
        localStorage.setItem("gastrolog:theme", t);
      }
      setPrefsLoaded(true);
    }
  }, [preferences.data, prefsLoaded]);

  const setTheme = useCallback(
    (t: Theme) => {
      setThemeLocal(t);
      localStorage.setItem("gastrolog:theme", t);
      putPreferences.mutate({ theme: t });
    },
    [putPreferences],
  );

  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => setSystemDark(e.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  const dark = theme === "dark" || (theme === "system" && systemDark);

  return { theme, setTheme, dark } as const;
}
