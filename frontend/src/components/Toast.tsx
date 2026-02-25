import {
  createContext,
  useContext,
  useState,
  useEffect,
  useSyncExternalStore,
} from "react";
import { ConnectError, Code } from "@connectrpc/connect";
import { useThemeClass } from "../hooks/useThemeClass";

type ToastLevel = "error" | "warn" | "info";

interface Toast {
  id: string;
  message: string;
  level: ToastLevel;
  createdAt: number;
}

// ---------------------------------------------------------------------------
// Module-scoped toast store — callable from anywhere (React or plain JS).
// ---------------------------------------------------------------------------

type Listener = () => void;
const listeners = new Set<Listener>();
let toasts: Toast[] = [];
let nextId = 0;
const timers = new Map<string, ReturnType<typeof setTimeout>>();

function notify() {
  for (const fn of listeners) fn();
}

function subscribe(fn: Listener) {
  listeners.add(fn);
  return () => {
    listeners.delete(fn);
  };
}

function getSnapshot() {
  return toasts;
}

/** Add a toast from anywhere — works outside the React tree. */
export function addToast(message: string, level: ToastLevel = "error") {
  if (toasts.some((t) => t.message === message && t.level === level)) return;

  nextId++;
  const id = `toast-${nextId}`;
  const toast: Toast = { id, message, level, createdAt: Date.now() };
  toasts = [...toasts, toast];

  const timeout = level === "error" ? 15000 : 8000;
  const timer = setTimeout(() => {
    timers.delete(id);
    toasts = toasts.filter((t) => t.id !== id);
    notify();
  }, timeout);
  timers.set(id, timer);

  notify();
  return id;
}

function dismissToast(id: string) {
  const timer = timers.get(id);
  if (timer) {
    clearTimeout(timer);
    timers.delete(id);
  }
  toasts = toasts.filter((t) => t.id !== id);
  notify();
}

/** Imperative error toaster — safe to call outside the React tree. */
export function toastError(err: unknown) {
  if (err instanceof ConnectError && err.code === Code.Unauthenticated) return;
  const message = err instanceof Error ? err.message : String(err);
  addToast(message, "error");
}

// ---------------------------------------------------------------------------
// React bindings
// ---------------------------------------------------------------------------

interface ToastContextValue {
  addToast: (message: string, level?: ToastLevel) => void;
  dismissToast: (id: string) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

export function useToast(): ToastContextValue {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within ToastProvider");
  return ctx;
}

export function ToastProvider({
  children,
  dark,
}: Readonly<{ children: React.ReactNode; dark: boolean }>) {
  const current = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  return (
    <ToastContext.Provider value={{ addToast, dismissToast }}>
      {children}
      <ToastOverlay toasts={current} onDismiss={dismissToast} dark={dark} />
    </ToastContext.Provider>
  );
}

function ToastOverlay({
  toasts,
  onDismiss,
  dark,
}: Readonly<{
  toasts: Toast[];
  onDismiss: (id: string) => void;
  dark: boolean;
}>) {
  if (toasts.length === 0) return null;

  return (
    <div role="status" aria-live="polite" className="fixed bottom-4 right-4 z-100 flex flex-col-reverse gap-2 max-w-sm">
      {toasts.map((toast) => (
        <ToastItem key={toast.id} toast={toast} onDismiss={onDismiss} dark={dark} />
      ))}
    </div>
  );
}

const LEVEL_STYLES: Record<ToastLevel, string> = {
  error: "border-l-severity-error",
  warn: "border-l-severity-warn",
  info: "border-l-severity-info",
};

function ToastItem({
  toast,
  onDismiss,
  dark,
}: Readonly<{
  toast: Toast;
  onDismiss: (id: string) => void;
  dark: boolean;
}>) {
  const [visible, setVisible] = useState(false);
  const c = useThemeClass(dark);
  const accent = LEVEL_STYLES[toast.level];

  useEffect(() => {
    const frame = requestAnimationFrame(() => setVisible(true));
    return () => cancelAnimationFrame(frame);
  }, []);

  return (
    <div
      className={`flex items-start gap-2 px-3 py-2.5 rounded border-l-3 shadow-lg ${c(
        "bg-ink-raised text-text-bright",
        "bg-light-raised text-light-text-bright",
      )} text-[0.85em] transition-all duration-200 ${accent} ${
        visible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-4"
      }`}
    >
      <span className="flex-1 wrap-break-word">{toast.message}</span>
      <button
        onClick={() => onDismiss(toast.id)}
        aria-label="Dismiss"
        className={`shrink-0 w-5 h-5 flex items-center justify-center rounded ${c(
          "text-text-muted hover:text-text-bright",
          "text-light-text-muted hover:text-light-text-bright",
        )} transition-colors`}
      >
        &times;
      </button>
    </div>
  );
}
