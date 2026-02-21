import { useEffect } from "react";
import { FocusTrap } from "focus-trap-react";
import { useThemeClass } from "../hooks/useThemeClass";

const sizeClasses = {
  sm: "w-full max-w-sm p-6",
  lg: "w-[90vw] max-w-4xl h-[80vh] p-6",
  xl: "w-[90vw] max-w-5xl h-[85vh] flex flex-col overflow-hidden",
} as const;

interface DialogProps {
  onClose: () => void;
  ariaLabel: string;
  dark: boolean;
  size?: "sm" | "lg" | "xl";
  children: React.ReactNode;
}

export function Dialog({
  onClose,
  ariaLabel,
  dark,
  size = "xl",
  children,
}: Readonly<DialogProps>) {
  const c = useThemeClass(dark);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    };
    globalThis.addEventListener("keydown", handler, true);
    return () => globalThis.removeEventListener("keydown", handler, true);
  }, [onClose]);

  const bg =
    size === "sm"
      ? c("bg-ink-surface border border-ink-border", "bg-light-surface border border-light-border")
      : c(
          "bg-ink-raised border border-ink-border-subtle",
          "bg-light-raised border border-light-border-subtle",
        );

  return (
    <FocusTrap
      focusTrapOptions={{ escapeDeactivates: false, allowOutsideClick: true }}
    >
      <div className="fixed inset-0 z-50 flex items-center justify-center">
        <button
          type="button"
          className="absolute inset-0 bg-black/40 cursor-default"
          onClick={onClose}
          aria-label="Close dialog"
          tabIndex={-1}
        />
        <div className="relative">
          <div
            role="dialog"
            aria-modal="true"
            aria-label={ariaLabel}
            className={`rounded-lg shadow-2xl ${sizeClasses[size]} ${bg}`}
          >
            {children}
          </div>
          <button
            onClick={onClose}
            aria-label="Close"
            className={`absolute -top-3 -right-3 w-7 h-7 flex items-center justify-center rounded-full text-lg leading-none shadow-lg border transition-colors ${c(
              "bg-ink-surface border-ink-border text-text-muted hover:text-text-bright hover:bg-ink-hover",
              "bg-light-surface border-light-border text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
            )}`}
          >
            &times;
          </button>
        </div>
      </div>
    </FocusTrap>
  );
}