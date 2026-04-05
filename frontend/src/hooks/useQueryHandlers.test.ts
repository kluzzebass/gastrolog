import { describe, test, expect, mock } from "bun:test";
import { renderHook, act } from "@testing-library/react";
import { useQueryHandlers } from "./useQueryHandlers";
import { SEVERITY_LEVELS } from "../lib/severity";

function makeDeps(overrides: Partial<Parameters<typeof useQueryHandlers>[0]> = {}) {
  return {
    q: "",
    setUrlQuery: mock(() => {}),
    navigate: mock(() => {}),
    selectedVault: "all",
    setSelectedVault: mock(() => {}),
    isFollowMode: false,
    isReversed: false,
    timeRange: "15m",
    followReversed: false,
    setFollowReversed: mock(() => {}),
    draft: "",
    setDraft: mock(() => {}),
    cursorRef: { current: 0 },
    queryInputRef: { current: null },
    ...overrides,
  } satisfies Parameters<typeof useQueryHandlers>[0];
}

// ── activeSeverities ─────────────────────────────────────────────────

describe("useQueryHandlers", () => {
  describe("activeSeverities", () => {
    test("returns empty when query has no level= tokens", () => {
      const deps = makeDeps({ q: "foo bar" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      expect(result.current.activeSeverities).toEqual([]);
    });

    test("returns matching levels from query", () => {
      const deps = makeDeps({ q: "level=error level=info some text" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      expect(result.current.activeSeverities).toEqual(["error", "info"]);
    });

    test("returns all levels when all are present", () => {
      const deps = makeDeps({
        q: "level=error level=warn level=info level=debug level=trace",
      });
      const { result } = renderHook(() => useQueryHandlers(deps));
      expect(result.current.activeSeverities).toEqual([...SEVERITY_LEVELS]);
    });

    test("allSeverities is the canonical list", () => {
      const deps = makeDeps();
      const { result } = renderHook(() => useQueryHandlers(deps));
      expect(result.current.allSeverities).toEqual(SEVERITY_LEVELS);
    });
  });

  // ── toggleSeverity ──────────────────────────────────────────────────

  describe("toggleSeverity", () => {
    test("adds a severity to empty query", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.toggleSeverity("error"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("level=error");
    });

    test("adds a second severity using OR group", () => {
      const deps = makeDeps({ q: "level=error" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.toggleSeverity("warn"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith(
        "(level=error OR level=warn)",
      );
    });

    test("removes a severity when already present (single)", () => {
      const deps = makeDeps({ q: "level=error" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.toggleSeverity("error"));
      // No severity tokens left — should be empty base
      expect(deps.setUrlQuery).toHaveBeenCalledWith("");
    });

    test("removes one severity from an OR group", () => {
      const deps = makeDeps({ q: "(level=error OR level=warn)" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.toggleSeverity("error"));
      // Only warn left — should be a single token, not a group
      expect(deps.setUrlQuery).toHaveBeenCalledWith("level=warn");
    });

    test("preserves non-severity tokens in query", () => {
      const deps = makeDeps({ q: "level=error foo=bar" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.toggleSeverity("warn"));
      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).toContain("foo=bar");
      expect(call).toContain("level=error");
      expect(call).toContain("level=warn");
    });
  });

  // ── handleSegmentClick ──────────────────────────────────────────────

  describe("handleSegmentClick", () => {
    test("clicking 'other' adds 'not level=*' when not present", () => {
      const deps = makeDeps({ q: "foo" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleSegmentClick("other"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("foo not level=*");
    });

    test("clicking 'other' when already present: regex \\b after * never matches, so it re-adds (known bug)", () => {
      // NOTE: The detection regex /\bnot\s+level=\*\b/ never matches because
      // \b after * (non-word char) fails. So the toggle always adds.
      const deps = makeDeps({ q: "foo not level=*" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleSegmentClick("other"));
      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).toContain("not level=*");
    });

    test("clicking a severity level delegates to toggleSeverity", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleSegmentClick("error"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("level=error");
    });
  });

  // ── handleFieldSelect ──────────────────────────────────────────────

  describe("handleFieldSelect", () => {
    test("adds a field token to empty query", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleFieldSelect("host", "web-1"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("host=web-1");
    });

    test("appends a field token to existing query", () => {
      const deps = makeDeps({ q: "foo" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleFieldSelect("host", "web-1"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("foo host=web-1");
    });

    test("removes a field token when already present", () => {
      const deps = makeDeps({ q: "foo host=web-1" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleFieldSelect("host", "web-1"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("foo");
    });

    test("quotes values with special characters", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleFieldSelect("msg", "hello world"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith('msg="hello world"');
    });

    test("does not quote simple alphanumeric values", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleFieldSelect("host", "web-1.example"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("host=web-1.example");
    });

    test("removes quoted token when already present", () => {
      const deps = makeDeps({ q: 'msg="hello world"' });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleFieldSelect("msg", "hello world"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("");
    });
  });

  // ── handleVaultSelect ──────────────────────────────────────────────

  describe("handleVaultSelect", () => {
    test("selects a vault and injects vault_id token", () => {
      const deps = makeDeps({ q: "foo", selectedVault: "all" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleVaultSelect("vault-abc"));
      expect(deps.setSelectedVault).toHaveBeenCalledWith("vault-abc");
      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).toContain("vault_id=vault-abc");
    });

    test("deselects vault when clicking the already-selected vault", () => {
      const deps = makeDeps({ q: "vault_id=vault-abc foo", selectedVault: "vault-abc" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleVaultSelect("vault-abc"));
      expect(deps.setSelectedVault).toHaveBeenCalledWith("all");
      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).not.toContain("vault_id=");
    });
  });

  // ── handleChunkSelect ──────────────────────────────────────────────

  describe("handleChunkSelect", () => {
    test("adds chunk token to query", () => {
      const deps = makeDeps({ q: "foo" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleChunkSelect("chunk-123"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("chunk=chunk-123 foo");
    });

    test("removes chunk token when already present", () => {
      const deps = makeDeps({ q: "chunk=chunk-123 foo" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleChunkSelect("chunk-123"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("foo");
    });

    test("adds chunk token to empty query", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleChunkSelect("chunk-abc"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("chunk=chunk-abc");
    });
  });

  // ── handlePosSelect ────────────────────────────────────────────────

  describe("handlePosSelect", () => {
    test("adds chunk and pos tokens", () => {
      const deps = makeDeps({ q: "foo" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handlePosSelect("chunk-1", "42"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("chunk=chunk-1 pos=42 foo");
    });

    test("removes chunk and pos tokens when pos already present", () => {
      const deps = makeDeps({ q: "chunk=chunk-1 pos=42 foo" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handlePosSelect("chunk-1", "42"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("foo");
    });

    test("replaces existing chunk/pos with new values", () => {
      const deps = makeDeps({ q: "chunk=old pos=99 foo" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handlePosSelect("new-chunk", "10"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("chunk=new-chunk pos=10 foo");
    });
  });

  // ── handleTokenToggle ──────────────────────────────────────────────

  describe("handleTokenToggle", () => {
    test("adds an arbitrary token to empty query", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleTokenToggle("json"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("json");
    });

    test("adds an arbitrary token to existing query", () => {
      const deps = makeDeps({ q: "foo" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleTokenToggle("json"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("foo json");
    });

    test("removes token when already present", () => {
      const deps = makeDeps({ q: "foo json bar" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleTokenToggle("json"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("foo bar");
    });

    test("handles token at start of query", () => {
      const deps = makeDeps({ q: "json bar" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleTokenToggle("json"));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("bar");
    });
  });

  // ── toggleReverse ──────────────────────────────────────────────────

  describe("toggleReverse", () => {
    test("in follow mode, calls setFollowReversed with toggler", () => {
      const deps = makeDeps({ isFollowMode: true });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.toggleReverse());
      expect(deps.setFollowReversed).toHaveBeenCalledTimes(1);
      // The argument should be a function that toggles
      const toggleFn = (deps.setFollowReversed as ReturnType<typeof mock>).mock.calls[0]![0] as (prev: boolean) => boolean;
      expect(toggleFn(false)).toBe(true);
      expect(toggleFn(true)).toBe(false);
    });

    test("in search mode with explicit start/end, flips reverse= token", () => {
      const deps = makeDeps({
        q: "start=2026-01-01T00:00:00Z end=2026-01-02T00:00:00Z reverse=false foo",
        isReversed: false,
      });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.toggleReverse());
      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).toContain("reverse=true");
      // Original start/end preserved
      expect(call).toContain("start=2026-01-01T00:00:00Z");
    });

    test("in search mode without explicit start/end, injects time range", () => {
      const deps = makeDeps({
        q: "foo bar",
        isReversed: false,
        timeRange: "15m",
      });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.toggleReverse());
      const call = (deps.setUrlQuery as ReturnType<typeof mock>).mock.calls[0]![0] as string;
      expect(call).toContain("reverse=true");
      expect(call).toContain("foo bar");
    });
  });

  // ── handleMultiFieldSelect ─────────────────────────────────────────

  describe("handleMultiFieldSelect", () => {
    test("adds multiple field tokens at once", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleMultiFieldSelect([["host", "web-1"], ["env", "prod"]]));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("host=web-1 env=prod");
    });

    test("skips tokens that already exist in query", () => {
      const deps = makeDeps({ q: "host=web-1" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleMultiFieldSelect([["host", "web-1"], ["env", "prod"]]));
      expect(deps.setUrlQuery).toHaveBeenCalledWith("host=web-1 env=prod");
    });

    test("quotes special-character values", () => {
      const deps = makeDeps({ q: "" });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleMultiFieldSelect([["msg", "hello world"]]));
      expect(deps.setUrlQuery).toHaveBeenCalledWith('msg="hello world"');
    });
  });

  // ── handleSpanClick ────────────────────────────────────────────────

  describe("handleSpanClick", () => {
    test("inserts value at cursor position in draft", () => {
      const deps = makeDeps({ draft: "hello world", cursorRef: { current: 5 } });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleSpanClick("inserted"));
      expect(deps.setDraft).toHaveBeenCalledWith("hello inserted world");
    });

    test("inserts at beginning of empty draft", () => {
      const deps = makeDeps({ draft: "", cursorRef: { current: 0 } });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleSpanClick("token"));
      expect(deps.setDraft).toHaveBeenCalledWith("token");
    });

    test("inserts at end of draft", () => {
      const deps = makeDeps({ draft: "foo", cursorRef: { current: 3 } });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleSpanClick("bar"));
      expect(deps.setDraft).toHaveBeenCalledWith("foo bar");
    });

    test("does not double spaces if already at a space boundary", () => {
      const deps = makeDeps({ draft: "foo ", cursorRef: { current: 4 } });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleSpanClick("bar"));
      expect(deps.setDraft).toHaveBeenCalledWith("foo bar");
    });

    test("updates cursorRef to end of inserted text", () => {
      const cursorRef = { current: 0 };
      const deps = makeDeps({ draft: "", cursorRef });
      const { result } = renderHook(() => useQueryHandlers(deps));
      act(() => result.current.handleSpanClick("abc"));
      expect(cursorRef.current).toBe(3);
    });
  });
});
