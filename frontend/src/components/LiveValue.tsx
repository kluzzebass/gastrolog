import type { ReactNode } from "react";
import { useThemeClass } from "../hooks/useThemeClass";
import { joinClasses, liveValueClass, reserveChars, type LiveAlign, type LiveTone } from "./liveValueStyle";

interface LiveValueProps {
  children: ReactNode;
  dark: boolean;
  tone?: LiveTone;
  align?: LiveAlign;
  /** Character cells to reserve: the formatter's `*_MAX_CHARS` contract. */
  reserve?: number;
  /** Size, weight or style additions only; tone and alignment have their own props. */
  className?: string;
  title?: string;
}

/**
 * LiveValue displays a value that updates in place. It owns the layout
 * guards a ticking value needs: tabular digits, no wrapping, alignment, and
 * a width reservation taken from the formatter's maximum output, so a
 * change in value never resizes the cell or moves its neighbours.
 */
export function LiveValue({
  children,
  dark,
  tone = "bright",
  align = "end",
  reserve,
  className,
  title,
}: Readonly<LiveValueProps>) {
  const c = useThemeClass(dark);
  return (
    <span
      className={joinClasses(liveValueClass(c, tone, align), className)}
      style={reserve === undefined ? undefined : reserveChars(reserve)}
      title={title}
    >
      {children}
    </span>
  );
}
