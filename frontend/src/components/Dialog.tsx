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
      <div
        className="fixed inset-0 z-50 flex items-center justify-center"
        onClick={onClose}
      >
        <div className="absolute inset-0 bg-black/40" />
        <div
          role="dialog"
          aria-modal="true"
          aria-label={ariaLabel}
          className={`relative rounded-lg shadow-2xl ${sizeClasses[size]} ${bg}`}
          onClick={(e) => e.stopPropagation()}
        >
          {children}
        </div>
      </div>
    </FocusTrap>
  );
}

export function CloseButton({
  onClick,
  dark,
}: Readonly<{
  onClick: () => void;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  return (
    <button
      onClick={onClick}
      aria-label="Close"
      className={`absolute top-3 right-3 w-7 h-7 flex items-center justify-center rounded text-lg leading-none transition-colors ${c(
        "text-text-muted hover:text-text-bright",
        "text-light-text-muted hover:text-light-text-bright",
      )}`}
    >
      &times;
    </button>
  );
}
