import { describe, test, expect } from "bun:test";
import { escapeHtml } from "./escapeHtml";

describe("escapeHtml", () => {
  test("neutralizes the five HTML-significant characters", () => {
    expect(escapeHtml(`&<>"'`)).toBe("&amp;&lt;&gt;&quot;&#39;");
  });

  test("ampersand is escaped before the entities it introduces", () => {
    expect(escapeHtml("&lt;")).toBe("&amp;lt;");
  });

  test("an injected element loses its tag delimiters", () => {
    const escaped = escapeHtml(`<img src=x onerror=alert(1)>`);
    expect(escaped).not.toContain("<");
    expect(escaped).not.toContain(">");
    expect(escaped).toBe("&lt;img src=x onerror=alert(1)&gt;");
  });

  test("an attribute break-out loses its quotes", () => {
    expect(escapeHtml(`" onmouseover="alert(1)`)).toBe("&quot; onmouseover=&quot;alert(1)");
    expect(escapeHtml(`' onmouseover='alert(1)`)).toBe("&#39; onmouseover=&#39;alert(1)");
  });

  test("escaped output decodes back to the original text", () => {
    const hostile = `<img src=x onerror=alert(1)> & "quoted" 'single'`;
    const el = document.createElement("div");
    el.innerHTML = escapeHtml(hostile);
    expect(el.textContent).toBe(hostile);
    expect(el.querySelector("img")).toBeNull();
  });

  test("ordinary text passes through unchanged", () => {
    expect(escapeHtml("api-gateway-01")).toBe("api-gateway-01");
    expect(escapeHtml("GET /v1/records?limit=50")).toBe("GET /v1/records?limit=50");
  });

  test("null and undefined render as empty, not as their names", () => {
    const missing: (string | null | undefined)[] = [null, void 0];
    for (const value of missing) {
      expect(escapeHtml(value)).toBe("");
    }
  });

  test("non-string values are stringified", () => {
    expect(escapeHtml(0)).toBe("0");
    expect(escapeHtml(42)).toBe("42");
    expect(escapeHtml(false)).toBe("false");
  });
});
