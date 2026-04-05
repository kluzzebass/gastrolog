/**
 * Scroll-related side effects extracted from useSearchView.
 * Infinite scroll, follow scroll tracking, post-search auto-scroll.
 */
import { useState, useRef, useEffect } from "react";
import type { MutableRefObject } from "react";
import type { Record as ProtoRecord } from "../api/client";

interface ScrollEffectDeps {
  isFollowMode: boolean;
  isSearching: boolean;
  hasMore: boolean;
  selectedRecord: ProtoRecord | null;
  recordsLength: number;

  // Stable refs to avoid effect churn
  loadMoreRef: MutableRefObject<(expr: string) => void>;
  resetFollowNewCountRef: MutableRefObject<() => void>;
  expressionRef: MutableRefObject<string>;

  // Shared ref owned by parent hook
  selectedRowRef: MutableRefObject<HTMLElement | null>;
}

export function useScrollEffects(deps: ScrollEffectDeps) {
  const {
    isFollowMode, isSearching, hasMore, selectedRecord, recordsLength,
    loadMoreRef, resetFollowNewCountRef, expressionRef,
    selectedRowRef,
  } = deps;

  const sentinelRef = useRef<HTMLDivElement>(null);
  const logScrollRef = useRef<HTMLDivElement>(null);
  const loadMoreGateRef = useRef(false);
  const [isScrolledDown, setIsScrolledDown] = useState(false);
  const scrollToSelectedRef = useRef(false);
  const prevSearchingRef = useRef(false);

  // Infinite scroll: observe a sentinel div at the bottom of the results.
  useEffect(() => {
    const sentinel = sentinelRef.current;
    const scrollEl = logScrollRef.current;
    if (!sentinel) return;

    const openGate = () => { loadMoreGateRef.current = true; };
    scrollEl?.addEventListener("scroll", openGate, { passive: true, once: true });

    const observer = new IntersectionObserver(
      (entries) => {
        if (
          entries[0]?.isIntersecting &&
          hasMore &&
          !isSearching &&
          loadMoreGateRef.current &&
          document.visibilityState === "visible"
        ) {
          loadMoreGateRef.current = false;
          loadMoreRef.current(expressionRef.current);
        }
      },
      { root: scrollEl, rootMargin: "0px 0px 200px 0px" },
    );
    observer.observe(sentinel);
    return () => {
      observer.disconnect();
      scrollEl?.removeEventListener("scroll", openGate);
    };
  }, [hasMore, isSearching]);

  // Follow mode: track scroll position and auto-reset new-record counter.
  useEffect(() => {
    const el = logScrollRef.current;
    if (!el || !isFollowMode) {
      setIsScrolledDown(false);
      return;
    }
    const onScroll = () => {
      const scrolled = el.scrollTop > 50;
      setIsScrolledDown(scrolled);
      if (!scrolled) resetFollowNewCountRef.current();
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, [isFollowMode]);

  // After search completes, scroll the selected row into view.
  useEffect(() => {
    if (prevSearchingRef.current && !isSearching) {
      if (selectedRowRef.current) {
        selectedRowRef.current.scrollIntoView({ block: "center" });
        scrollToSelectedRef.current = false;
      } else if (selectedRecord && hasMore) {
        scrollToSelectedRef.current = true;
        loadMoreRef.current(expressionRef.current);
      }
    }
    prevSearchingRef.current = isSearching;
  }, [isSearching]); // eslint-disable-line react-hooks/exhaustive-deps

  // When new records arrive during auto-pagination, check if selected row appeared.
  useEffect(() => {
    if (!scrollToSelectedRef.current || isSearching) return;
    if (selectedRowRef.current) {
      selectedRowRef.current.scrollIntoView({ block: "center" });
      scrollToSelectedRef.current = false;
    } else if (hasMore) {
      loadMoreRef.current(expressionRef.current);
    } else {
      scrollToSelectedRef.current = false;
    }
  }, [recordsLength]); // eslint-disable-line react-hooks/exhaustive-deps

  /** Reset scroll state when starting a new search. */
  const resetScroll = () => {
    loadMoreGateRef.current = false;
    scrollToSelectedRef.current = false;
    logScrollRef.current?.scrollTo(0, 0);
  };

  return {
    sentinelRef,
    logScrollRef,
    isScrolledDown,
    scrollToSelectedRef,
    resetScroll,
  };
}
