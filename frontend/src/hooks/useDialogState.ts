import { useState, useRef, useEffect } from "react";
import { useIsFetching } from "@tanstack/react-query";

export function useDialogState() {
  const [showPlan, setShowPlan] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [showSavedQueries, setShowSavedQueries] = useState(false);
  const [showChangePassword, setShowChangePassword] = useState(false);
  // Inspector glow effect: briefly flash when any fetch is active.
  const fetchCount = useIsFetching();
  const [inspectorGlow, setInspectorGlow] = useState(false);
  const glowTimer = useRef<ReturnType<typeof setTimeout>>(null);
  useEffect(() => {
    if (fetchCount > 0) {
      setInspectorGlow(true);
      if (glowTimer.current) clearTimeout(glowTimer.current);
      glowTimer.current = setTimeout(() => setInspectorGlow(false), 800);
    }
  }, [fetchCount]);

  return {
    showPlan,
    setShowPlan,
    showHistory,
    setShowHistory,
    showSavedQueries,
    setShowSavedQueries,
    showChangePassword,
    setShowChangePassword,
    inspectorGlow,
  };
}
