import { useRef, useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useClickOutside } from "../../hooks/useClickOutside";

type ButtonVariant = "primary" | "ghost" | "danger";

interface ButtonProps {
  onClick: () => void;
  dark?: boolean;
  variant?: ButtonVariant;
  bordered?: boolean;
  disabled?: boolean;
  children: React.ReactNode;
  className?: string;
}

const baseClass = "px-3 py-1.5 text-[0.8em] rounded transition-colors disabled:opacity-50";

export function Button({
  onClick,
  dark = true,
  variant = "primary",
  bordered,
  disabled,
  children,
  className: extra,
}: Readonly<ButtonProps>) {
  const c = useThemeClass(dark);

  let variantClass: string;
  switch (variant) {
    case "primary":
      variantClass = "bg-copper text-text-on-copper hover:bg-copper-glow";
      break;
    case "danger":
      variantClass = "bg-severity-error text-severity-error-text hover:brightness-110";
      break;
    case "ghost":
      variantClass = bordered
        ? c(
            "border border-ink-border text-text-muted hover:text-text-bright hover:bg-ink-hover",
            "border border-light-border text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
          )
        : c(
            "text-text-muted hover:text-text-bright hover:bg-ink-hover",
            "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
          );
      break;
  }

  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`${baseClass} ${variantClass}${extra ? " " + extra : ""}`}
    >
      {children}
    </button>
  );
}

interface DropdownItem {
  value: string;
  label: string;
}

interface DropdownButtonProps {
  label: string;
  items: DropdownItem[];
  onSelect: (value: string) => void;
  dark?: boolean;
  dropUp?: boolean;
}

export function DropdownButton({
  label,
  items,
  onSelect,
  dark = true,
  dropUp = false,
}: Readonly<DropdownButtonProps>) {
  const c = useThemeClass(dark);
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useClickOutside(ref, () => setOpen(false));

  return (
    <div ref={ref} className="relative">
      <Button onClick={() => setOpen((prev) => !prev)} dark={dark}>
        {label}
      </Button>
      {open && (
        <div
          className={`absolute right-0 min-w-[10rem] rounded border shadow-lg z-50 py-1 ${dropUp ? "bottom-full mb-1" : "top-full mt-1"} ${c(
            "bg-ink-surface border-ink-border",
            "bg-light-surface border-light-border",
          )}`}
        >
          {items.map((item) => (
            <button
              key={item.value}
              onClick={() => {
                setOpen(false);
                onSelect(item.value);
              }}
              className={`w-full text-left px-3 py-1.5 text-[0.85em] transition-colors ${c(
                "text-text-bright hover:bg-ink-hover",
                "text-light-text-bright hover:bg-light-hover",
              )}`}
            >
              {item.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

