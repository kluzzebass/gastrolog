import { useState, useRef } from "react";
import { useThemeClass } from "../hooks/useThemeClass";
import { CopyIcon, CheckIcon } from "./icons";

export function CopyButton({
  text,
  dark,
  className = "",
}: Readonly<{
  text: string;
  dark: boolean;
  className?: string;
}>) {
  const c = useThemeClass(dark);
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    navigator.clipboard.writeText(text);
    setCopied(true);
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => setCopied(false), 1500);
  };

  const color = copied
    ? "text-severity-info"
    : c("text-text-ghost hover:text-copper", "text-light-text-ghost hover:text-copper");

  return (
    <button
      type="button"
      onClick={handleClick}
      className={`inline-flex items-center justify-center transition-colors cursor-pointer ${color} ${className}`}
      aria-label={copied ? "Copied" : "Copy to clipboard"}
      title={copied ? "Copied!" : "Copy to clipboard"}
    >
      {copied ? (
        <CheckIcon className="w-3.5 h-3.5" />
      ) : (
        <CopyIcon className="w-3.5 h-3.5" />
      )}
    </button>
  );
}
