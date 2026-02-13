import { describe, expect, test } from "bun:test";
import { useThemeClass } from "./useThemeClass";

describe("useThemeClass", () => {
  test("dark mode returns dark class", () => {
    const c = useThemeClass(true);
    expect(c("dark-class", "light-class")).toBe("dark-class");
  });

  test("light mode returns light class", () => {
    const c = useThemeClass(false);
    expect(c("dark-class", "light-class")).toBe("light-class");
  });
});
