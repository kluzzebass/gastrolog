import { useState, useRef, useEffect } from "react";
import { useIsFetching } from "@tanstack/react-query";
import type { SettingsTab } from "../components/settings/SettingsDialog";
import type { InspectorTab } from "../components/inspector/InspectorDialog";

export function useDialogState() {
  const [showPlan, setShowPlan] = useState(false);
  const [showHelp, setShowHelp] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [settingsTab, setSettingsTab] = useState<SettingsTab>("service");
  const [showInspector, setShowInspector] = useState(false);
  const [inspectorTab, setInspectorTab] = useState<InspectorTab>("stores");
  const [showHistory, setShowHistory] = useState(false);
  const [showSavedQueries, setShowSavedQueries] = useState(false);
  const [showChangePassword, setShowChangePassword] = useState(false);
  const [showHelpDialog, setShowHelpDialog] = useState(false);
  const [helpTopic, setHelpTopic] = useState<string | undefined>(undefined);

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
    showHelp,
    setShowHelp,
    showSettings,
    setShowSettings,
    settingsTab,
    setSettingsTab,
    showInspector,
    setShowInspector,
    inspectorTab,
    setInspectorTab,
    showHistory,
    setShowHistory,
    showSavedQueries,
    setShowSavedQueries,
    showChangePassword,
    setShowChangePassword,
    showHelpDialog,
    setShowHelpDialog,
    helpTopic,
    setHelpTopic,
    inspectorGlow,
  };
}
