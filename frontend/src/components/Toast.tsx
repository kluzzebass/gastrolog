import {
  createContext,
  useContext,
  useState,
  useEffect,
  useRef,
} from "react";

type ToastLevel = "error" | "warn" | "info";

interface Toast {
  id: string;
  message: string;
  level: ToastLevel;
  createdAt: number;
}

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

let nextId = 0;

export function ToastProvider({ children }: Readonly<{ children: React.ReactNode }>) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const timersRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(
    new Map(),
  );
  const toastsRef = useRef(toasts);
  toastsRef.current = toasts;

  const dismissToast = (id: string) => {
    const timer = timersRef.current.get(id);
    if (timer) {
      clearTimeout(timer);
      timersRef.current.delete(id);
    }
    setToasts((prev) => prev.filter((t) => t.id !== id));
  };

  const addToast = (message: string, level: ToastLevel = "error") => {
    // Deduplicate: skip if a toast with the same message and level already exists.
    if (toastsRef.current.some((t) => t.message === message && t.level === level)) {
      return;
    }

    nextId++;
    const id = `toast-${nextId}`;
    const toast: Toast = { id, message, level, createdAt: Date.now() };
    setToasts((prev) => [...prev, toast]);

    const timeout = level === "error" ? 15000 : 8000;
    const timer = setTimeout(() => {
      timersRef.current.delete(id);
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, timeout);
    timersRef.current.set(id, timer);

    return id;
  };

  // Cleanup timers on unmount.
  useEffect(() => {
    const timers = timersRef.current;
    return () => {
      for (const timer of timers.values()) {
        clearTimeout(timer);
      }
    };
  }, []);

  return (
    <ToastContext.Provider value={{ addToast, dismissToast }}>
      {children}
      <ToastOverlay toasts={toasts} onDismiss={dismissToast} />
    </ToastContext.Provider>
  );
}

function ToastOverlay({
  toasts,
  onDismiss,
}: Readonly<{
  toasts: Toast[];
  onDismiss: (id: string) => void;
}>) {
  if (toasts.length === 0) return null;

  return (
    <div role="status" aria-live="polite" className="fixed bottom-4 right-4 z-100 flex flex-col-reverse gap-2 max-w-sm">
      {toasts.map((toast) => (
        <ToastItem key={toast.id} toast={toast} onDismiss={onDismiss} />
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
}: Readonly<{
  toast: Toast;
  onDismiss: (id: string) => void;
}>) {
  const [visible, setVisible] = useState(false);
  const accent = LEVEL_STYLES[toast.level];

  // Animate in on mount.
  useEffect(() => {
    const frame = requestAnimationFrame(() => setVisible(true));
    return () => cancelAnimationFrame(frame);
  }, []);

  return (
    <div
      className={`flex items-start gap-2 px-3 py-2.5 rounded border-l-3 shadow-lg bg-ink-raised text-text-bright text-[0.85em] transition-all duration-200 ${accent} ${
        visible ? "opacity-100 translate-y-0" : "opacity-0 translate-y-4"
      }`}
    >
      <span className="flex-1 wrap-break-word">{toast.message}</span>
      <button
        onClick={() => onDismiss(toast.id)}
        aria-label="Dismiss"
        className="shrink-0 w-5 h-5 flex items-center justify-center rounded text-text-muted hover:text-text-bright transition-colors"
      >
        &times;
      </button>
    </div>
  );
}
