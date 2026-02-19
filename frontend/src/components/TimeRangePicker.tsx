import { useState, useEffect } from "react";
import { useThemeClass } from "../hooks/useThemeClass";
import {
  startOfMonth,
  endOfMonth,
  startOfWeek,
  endOfWeek,
  eachDayOfInterval,
  format,
  isSameDay,
  isSameMonth,
  isWithinInterval,
  addMonths,
  subMonths,
  isBefore,
} from "date-fns";
import { timeRangeMs } from "../utils";

export function TimeRangePicker({
  dark,
  rangeStart,
  rangeEnd,
  activePreset,
  onPresetClick,
  onApply,
}: Readonly<{
  dark: boolean;
  rangeStart: Date | null;
  rangeEnd: Date | null;
  activePreset: string;
  onPresetClick: (preset: string) => void;
  onApply: (start: Date, end: Date) => void;
}>) {
  const c = useThemeClass(dark);
  const [viewMonth, setViewMonth] = useState(() => rangeEnd ?? new Date());
  const [pendingStart, setPendingStart] = useState<Date | null>(rangeStart);
  const [pendingEnd, setPendingEnd] = useState<Date | null>(rangeEnd);
  const [startTime, setStartTime] = useState(() =>
    rangeStart ? format(rangeStart, "HH:mm") : "00:00",
  );
  const [endTime, setEndTime] = useState(() =>
    rangeEnd ? format(rangeEnd, "HH:mm") : "23:59",
  );
  const [picking, setPicking] = useState<"start" | "end">("start");

  // Sync from parent when presets or histogram brush update the range.
  useEffect(() => {
    setPendingStart(rangeStart);
    setPendingEnd(rangeEnd);
    if (rangeStart) setStartTime(format(rangeStart, "HH:mm"));
    if (rangeEnd) setEndTime(format(rangeEnd, "HH:mm"));
    if (rangeEnd) setViewMonth(rangeEnd);
    setPicking("start");
  }, [rangeStart, rangeEnd]);

  const handleDayClick = (day: Date) => {
    if (picking === "start") {
      setPendingStart(day);
      setPendingEnd(null);
      setPicking("end");
    } else {
      let s = pendingStart!;
      let e = day;
      if (isBefore(e, s)) [s, e] = [e, s];
      setPendingStart(s);
      setPendingEnd(e);
      setPicking("start");
    }
  };

  const handleApply = () => {
    if (!pendingStart || !pendingEnd) return;
    const [sh = 0, sm = 0] = startTime.split(":").map(Number);
    const [eh = 0, em = 0] = endTime.split(":").map(Number);
    const start = new Date(pendingStart);
    start.setHours(sh, sm, 0, 0);
    const end = new Date(pendingEnd);
    end.setHours(eh, em, 59, 999);
    onApply(start, end);
  };

  // Calendar grid
  const monthStart = startOfMonth(viewMonth);
  const monthEnd = endOfMonth(viewMonth);
  const calStart = startOfWeek(monthStart, { weekStartsOn: 1 });
  const calEnd = endOfWeek(monthEnd, { weekStartsOn: 1 });
  const days = eachDayOfInterval({ start: calStart, end: calEnd });
  const today = new Date();

  const presets = Object.keys(timeRangeMs);

  return (
    <div className="space-y-2.5">
      {/* Preset buttons */}
      <div className="flex flex-wrap gap-1">
        {[...presets, "All"].map((range) => (
          <button
            key={range}
            onClick={() => onPresetClick(range)}
            className={`px-2 py-0.5 text-[0.75em] font-mono rounded transition-all duration-150 ${
              activePreset === range
                ? c("bg-copper text-ink", "bg-copper text-white")
                : c(
                    "text-text-muted hover:text-text-normal hover:bg-ink-hover",
                    "text-light-text-muted hover:text-light-text-normal hover:bg-light-hover",
                  )
            }`}
          >
            {range}
          </button>
        ))}
      </div>

      {/* From / To display */}
      <div className="space-y-1">
        <div className="flex items-center gap-1.5">
          <span
            className={`text-[0.7em] w-8 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            From
          </span>
          <span
            className={`flex-1 text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {pendingStart ? format(pendingStart, "yyyy-MM-dd") : "\u2014"}
          </span>
          <input
            type="text"
            value={startTime}
            onChange={(e) => {
              const v = e.target.value.replace(/[^0-9:]/g, "");
              if (v.length <= 5) setStartTime(v);
            }}
            placeholder="HH:mm"
            className={`text-[0.75em] font-mono w-14 px-1 py-0.5 rounded border text-center ${c(
              "bg-ink-surface border-ink-border text-text-normal",
              "bg-light-surface border-light-border text-light-text-normal",
            )}`}
          />
        </div>
        <div className="flex items-center gap-1.5">
          <span
            className={`text-[0.7em] w-8 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            To
          </span>
          <span
            className={`flex-1 text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {pendingEnd ? format(pendingEnd, "yyyy-MM-dd") : "\u2014"}
          </span>
          <input
            type="text"
            value={endTime}
            onChange={(e) => {
              const v = e.target.value.replace(/[^0-9:]/g, "");
              if (v.length <= 5) setEndTime(v);
            }}
            placeholder="HH:mm"
            className={`text-[0.75em] font-mono w-14 px-1 py-0.5 rounded border text-center ${c(
              "bg-ink-surface border-ink-border text-text-normal",
              "bg-light-surface border-light-border text-light-text-normal",
            )}`}
          />
        </div>
      </div>

      {/* Month navigation */}
      <div className="flex items-center justify-between">
        <button
          onClick={() => setViewMonth((m) => subMonths(m, 1))}
          className={`text-[0.8em] px-1 rounded ${c("text-text-ghost hover:text-text-muted", "text-light-text-ghost hover:text-light-text-muted")}`}
        >
          {"\u25C2"}
        </button>
        <span
          className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {format(viewMonth, "MMMM yyyy")}
        </span>
        <button
          onClick={() => setViewMonth((m) => addMonths(m, 1))}
          className={`text-[0.8em] px-1 rounded ${c("text-text-ghost hover:text-text-muted", "text-light-text-ghost hover:text-light-text-muted")}`}
        >
          {"\u25B8"}
        </button>
      </div>

      {/* Calendar grid */}
      <div>
        <div className="grid grid-cols-7 gap-px mb-0.5">
          {["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"].map((d) => (
            <div
              key={d}
              className={`text-center text-[0.65em] font-mono ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              {d}
            </div>
          ))}
        </div>
        <div className="grid grid-cols-7 gap-px">
          {days.map((day) => {
            const inMonth = isSameMonth(day, viewMonth);
            const isToday = isSameDay(day, today);
            const isStart = pendingStart && isSameDay(day, pendingStart);
            const isEnd = pendingEnd && isSameDay(day, pendingEnd);
            const inRange =
              pendingStart &&
              pendingEnd &&
              isWithinInterval(day, {
                start: pendingStart,
                end: pendingEnd,
              });
            const selected = isStart || isEnd;

            return (
              <button
                key={day.toISOString()}
                onClick={() => handleDayClick(day)}
                className={`text-center text-[0.7em] font-mono py-0.5 rounded transition-colors ${
                  selected
                    ? "bg-copper text-white"
                    : inRange
                      ? c(
                          "bg-copper/10 text-text-normal",
                          "bg-copper/10 text-light-text-normal",
                        )
                      : inMonth
                        ? c(
                            "text-text-muted hover:bg-ink-hover hover:text-text-normal",
                            "text-light-text-muted hover:bg-light-hover hover:text-light-text-normal",
                          )
                        : c("text-text-ghost/40", "text-light-text-ghost/40")
                }${isToday && !selected ? ` ${c("underline decoration-copper", "underline decoration-copper")}` : ""}`}
              >
                {format(day, "d")}
              </button>
            );
          })}
        </div>
      </div>

      {/* Apply button */}
      <button
        onClick={handleApply}
        disabled={!pendingStart || !pendingEnd}
        className="w-full py-1 text-[0.8em] font-medium rounded bg-copper text-white hover:bg-copper-glow transition-all duration-200 disabled:opacity-30 disabled:cursor-not-allowed"
      >
        Apply
      </button>
    </div>
  );
}
