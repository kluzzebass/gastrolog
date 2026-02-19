import { useState, useCallback } from "react";
import { useNavigate } from "@tanstack/react-router";
import { timeRangeMs } from "../utils";
import { injectTimeRange, stripTimeRange } from "../utils/queryHelpers";

export function useTimeRange(q: string, isReversed: boolean) {
  const navigate = useNavigate({ from: "/search" });
  const [timeRange, setTimeRange] = useState("5m");
  const [rangeStart, setRangeStart] = useState<Date | null>(null);
  const [rangeEnd, setRangeEnd] = useState<Date | null>(null);

  const handleTimeRange = useCallback(
    (range: string) => {
      setTimeRange(range);
      if (range === "All") {
        setRangeStart(null);
        setRangeEnd(null);
      } else {
        const ms = timeRangeMs[range];
        if (ms) {
          const now = new Date();
          setRangeStart(new Date(now.getTime() - ms));
          setRangeEnd(now);
        }
      }
      const newQuery = injectTimeRange(q, range, isReversed);
      // Time ranges imply search mode — switch away from follow if active.
      navigate({ to: "/search", search: (prev: Record<string, unknown>) => ({ ...prev, q: newQuery }), replace: false } as any);
    },
    [q, isReversed, navigate],
  );

  const handleCustomRange = useCallback(
    (start: Date, end: Date) => {
      setTimeRange("custom");
      setRangeStart(start);
      setRangeEnd(end);
      const tokens = `start=${start.toISOString()} end=${end.toISOString()} reverse=${isReversed}`;
      const base = stripTimeRange(q);
      const newQuery = base ? `${tokens} ${base}` : tokens;
      // Time ranges imply search mode — switch away from follow if active.
      navigate({ to: "/search", search: (prev: Record<string, unknown>) => ({ ...prev, q: newQuery }), replace: false } as any);
    },
    [q, isReversed, navigate],
  );

  return {
    timeRange,
    setTimeRange,
    rangeStart,
    setRangeStart,
    rangeEnd,
    setRangeEnd,
    handleTimeRange,
    handleCustomRange,
  };
}
