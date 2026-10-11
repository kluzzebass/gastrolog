import { useHelp } from "../hooks/useHelp";

/**
 * Inline text that opens a help section, keeping the caller's text style.
 * Safe inside a clickable card header: the click does not reach the header.
 */
export function HelpLink({
  helpRef,
  title,
  className,
  children,
}: Readonly<{
  helpRef: string;
  title?: string;
  className?: string;
  children: React.ReactNode;
}>) {
  const { openHelp } = useHelp();
  return (
    <button
      type="button"
      title={title}
      className={`hover:underline cursor-pointer${className ? " " + className : ""}`}
      onClick={(e) => { e.stopPropagation(); openHelp(helpRef); }}
    >
      {children}
    </button>
  );
}
