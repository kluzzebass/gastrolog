import type { useThemeClass } from "../hooks/useThemeClass";

/** Text level of a live value; "inherit" takes the parent's color (badges, pills). */
export type LiveTone = "bright" | "normal" | "muted" | "info" | "warn" | "error" | "inherit";

export type LiveAlign = "start" | "end";

type ThemeClass = ReturnType<typeof useThemeClass>;

const TONE_CLASS: Record<LiveTone, (c: ThemeClass) => string> = {
  bright: (c) => c("text-text-bright", "text-light-text-bright"),
  normal: (c) => c("text-text-normal", "text-light-text-normal"),
  muted: (c) => c("text-text-muted", "text-light-text-muted"),
  info: () => "text-severity-info",
  warn: () => "text-severity-warn",
  error: () => "text-severity-error",
  inherit: () => "",
};

const ALIGN_CLASS: Record<LiveAlign, string> = {
  start: "text-left",
  end: "text-right",
};

/** The classes every live value carries: monospaced tabular digits that never wrap. */
export const LIVE_VALUE_BASE = "inline-block font-mono tabular-nums whitespace-nowrap";

/** Digit stability for live numbers set in the body face, where a mono cell would restyle the text. */
export const LIVE_TEXT = "tabular-nums whitespace-nowrap";

/**
 * Row class for a grid whose column tracks belong to its container. Rows
 * join the container's tracks, so a `max-content` track is sized by the
 * widest reservation across every row at once and stays put while values
 * change.
 */
export const LIVE_GRID_ROW = "col-span-full grid grid-cols-subgrid";

export function joinClasses(...classes: (string | undefined)[]): string {
  return classes.filter(Boolean).join(" ");
}

/** The class set of a LiveValue, for a value that must render as another element. */
export function liveValueClass(c: ThemeClass, tone: LiveTone = "bright", align: LiveAlign = "end"): string {
  return joinClasses(LIVE_VALUE_BASE, ALIGN_CLASS[align], TONE_CLASS[tone](c));
}

/**
 * Inline style reserving room for `chars` characters. In a monospaced face
 * every glyph advances exactly 1ch, so the reservation holds any string a
 * formatter's `*_MAX_CHARS` contract allows.
 */
export function reserveChars(chars: number): { minWidth: string } {
  return { minWidth: `${chars}ch` };
}
