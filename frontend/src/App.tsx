import { Outlet } from "@tanstack/react-router";
import { useThemeSync } from "./hooks/useThemeSync";
import { useThemeClass } from "./hooks/useThemeClass";
import { ToastProvider } from "./components/Toast";
import { useWatchConfig } from "./api/hooks/useWatchConfig";

export function App() {
  const { dark } = useThemeSync();
  const c = useThemeClass(dark);
  useWatchConfig();

  return (
    <ToastProvider dark={dark}>
      <div
        className={`grain h-screen overflow-hidden flex flex-col font-body text-base ${c(
          "bg-ink text-text-normal",
          "light-theme bg-light-bg text-light-text-normal",
        )}`}
      >
        <Outlet />
      </div>
    </ToastProvider>
  );
}
