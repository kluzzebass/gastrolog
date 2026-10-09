import { describe, expect, test } from "bun:test";
import { render } from "@testing-library/react";
import { LiveValue } from "./LiveValue";
import { LIVE_GRID_ROW, liveValueClass, reserveChars } from "./liveValueStyle";
import { BYTES_PER_SEC_MAX_CHARS, formatBytesPerSec } from "../utils/units";

function renderValue(ui: React.ReactElement): HTMLElement {
  const { container } = render(ui);
  return container.firstElementChild as HTMLElement;
}

function classesOf(el: HTMLElement): string[] {
  return el.className.split(/\s+/).filter(Boolean);
}

describe("LiveValue", () => {
  test("carries the layout guards every live value needs", () => {
    const el = renderValue(<LiveValue dark>{"1.5K/s"}</LiveValue>);
    const classes = classesOf(el);
    for (const guard of ["font-mono", "tabular-nums", "whitespace-nowrap", "inline-block"]) {
      expect(classes).toContain(guard);
    }
  });

  test("right-aligns by default so digits keep their columns", () => {
    const el = renderValue(<LiveValue dark>{"42"}</LiveValue>);
    expect(classesOf(el)).toContain("text-right");
    expect(classesOf(el)).not.toContain("text-left");
  });

  test("start alignment is available for values that lead a row", () => {
    const el = renderValue(<LiveValue dark align="start">{"42"}</LiveValue>);
    expect(classesOf(el)).toContain("text-left");
    expect(classesOf(el)).not.toContain("text-right");
  });

  test("reserves the formatter's maximum width in character cells", () => {
    const el = renderValue(
      <LiveValue dark reserve={BYTES_PER_SEC_MAX_CHARS}>
        {formatBytesPerSec(0)}
      </LiveValue>,
    );
    expect(el.style.minWidth).toBe(`${BYTES_PER_SEC_MAX_CHARS}ch`);
  });

  test("the reservation does not depend on the value shown", () => {
    const short = renderValue(<LiveValue dark reserve={12}>{formatBytesPerSec(0)}</LiveValue>);
    const long = renderValue(<LiveValue dark reserve={12}>{formatBytesPerSec(1023.9 * 1024)}</LiveValue>);
    expect(short.style.minWidth).toBe(long.style.minWidth);
  });

  test("reserves nothing when no width contract is given", () => {
    const el = renderValue(<LiveValue dark>{"1,234"}</LiveValue>);
    expect(el.style.minWidth).toBe("");
  });

  test("tone picks the theme's text level", () => {
    expect(classesOf(renderValue(<LiveValue dark>{"1"}</LiveValue>))).toContain("text-text-bright");
    expect(classesOf(renderValue(<LiveValue dark={false}>{"1"}</LiveValue>))).toContain(
      "text-light-text-bright",
    );
    expect(classesOf(renderValue(<LiveValue dark tone="muted">{"1"}</LiveValue>))).toContain(
      "text-text-muted",
    );
    expect(classesOf(renderValue(<LiveValue dark tone="warn">{"1"}</LiveValue>))).toContain(
      "text-severity-warn",
    );
  });

  test("inherit tone leaves the color to the parent", () => {
    const classes = classesOf(renderValue(<LiveValue dark tone="inherit">{"1"}</LiveValue>));
    expect(classes.some((cls) => cls.startsWith("text-text-") || cls.startsWith("text-severity-"))).toBe(false);
  });

  test("className adds to the guards rather than replacing them", () => {
    const classes = classesOf(renderValue(<LiveValue dark className="text-[0.9em]">{"1"}</LiveValue>));
    expect(classes).toContain("text-[0.9em]");
    expect(classes).toContain("whitespace-nowrap");
  });
});

const darkTheme = (darkCls: string) => darkCls;

describe("liveValueClass", () => {
  test("is the same class set LiveValue renders", () => {
    const c = darkTheme;
    const el = renderValue(<LiveValue dark tone="muted">{"1"}</LiveValue>);
    expect(el.className).toBe(liveValueClass(c, "muted"));
  });
});

describe("reserveChars", () => {
  test("expresses the reservation in ch, the advance of one monospaced glyph", () => {
    expect(reserveChars(8)).toEqual({ minWidth: "8ch" });
  });
});

describe("LIVE_GRID_ROW", () => {
  test("joins the container's column tracks", () => {
    expect(LIVE_GRID_ROW.split(" ")).toEqual(["col-span-full", "grid", "grid-cols-subgrid"]);
  });
});
