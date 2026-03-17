import { useState } from "react";

export function useDialogState() {
  const [showPlan, setShowPlan] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [showSavedQueries, setShowSavedQueries] = useState(false);
  const [showChangePassword, setShowChangePassword] = useState(false);
  const [showPreferences, setShowPreferences] = useState(false);

  return {
    showPlan,
    setShowPlan,
    showHistory,
    setShowHistory,
    showSavedQueries,
    setShowSavedQueries,
    showChangePassword,
    setShowChangePassword,
    showPreferences,
    setShowPreferences,
  };
}
