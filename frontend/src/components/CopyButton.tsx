import { useState, useCallback, useRef } from "react";
import { CopyIcon, CheckIcon, LinkIcon } from "./icons";

export function CopyButton({
  text,
  dark,
  className = "",
}: {
  text: string;
  dark: boolean;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      navigator.clipboard.writeText(text);
      setCopied(true);
      if (timerRef.current) clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => setCopied(false), 1500);
    },
    [text],
  );

  const color = copied
    ? "text-severity-info"
    : dark
      ? "text-text-ghost hover:text-copper"
      : "text-light-text-ghost hover:text-copper";

  return (
    <button
      type="button"
      onClick={handleClick}
      className={`inline-flex items-center justify-center transition-colors cursor-pointer ${color} ${className}`}
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

export function CopyLinkButton({
  url,
  dark,
  className = "",
}: {
  url: string;
  dark: boolean;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      navigator.clipboard.writeText(url);
      setCopied(true);
      if (timerRef.current) clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => setCopied(false), 1500);
    },
    [url],
  );

  const color = copied
    ? "text-severity-info"
    : dark
      ? "text-text-ghost hover:text-text-muted"
      : "text-light-text-ghost hover:text-light-text-muted";

  return (
    <button
      type="button"
      onClick={handleClick}
      className={`inline-flex items-center justify-center transition-colors cursor-pointer ${color} ${className}`}
      title={copied ? "Link copied!" : "Copy link to record"}
    >
      {copied ? (
        <CheckIcon className="w-3.5 h-3.5" />
      ) : (
        <LinkIcon className="w-3.5 h-3.5" />
      )}
    </button>
  );
}
