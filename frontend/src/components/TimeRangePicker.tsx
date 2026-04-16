import { useReducer } from "react";
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

// ── Picker reducer ───────────────────────────────────────────────────

interface PickerState {
  viewMonth: Date;
  pendingStart: Date | null;
  pendingEnd: Date | null;
  startTime: string;
  endTime: string;
  picking: "start" | "end";
  prevRange: { start: Date | null; end: Date | null };
}

type PickerAction =
  | { type: "sync"; rangeStart: Date | null; rangeEnd: Date | null }
  | { type: "pickDay"; day: Date }
  | { type: "prevMonth" }
  | { type: "nextMonth" }
  | { type: "setStartTime"; value: string }
  | { type: "setEndTime"; value: string };

function pickerReducer(state: PickerState, action: PickerAction): PickerState {
  switch (action.type) {
    case "sync": {
      const { rangeStart, rangeEnd } = action;
      return {
        ...state,
        prevRange: { start: rangeStart, end: rangeEnd },
        pendingStart: rangeStart,
        pendingEnd: rangeEnd,
        startTime: rangeStart ? format(rangeStart, "HH:mm") : state.startTime,
        endTime: rangeEnd ? format(rangeEnd, "HH:mm") : state.endTime,
        viewMonth: rangeEnd ?? state.viewMonth,
        picking: "start",
      };
    }
    case "pickDay": {
      const { day } = action;
      if (state.picking === "start") {
        return { ...state, pendingStart: day, pendingEnd: null, picking: "end" };
      }
      let s = state.pendingStart!;
      let e = day;
      if (isBefore(e, s)) [s, e] = [e, s];
      return { ...state, pendingStart: s, pendingEnd: e, picking: "start" };
    }
    case "prevMonth":
      return { ...state, viewMonth: subMonths(state.viewMonth, 1) };
    case "nextMonth":
      return { ...state, viewMonth: addMonths(state.viewMonth, 1) };
    case "setStartTime":
      return { ...state, startTime: action.value };
    case "setEndTime":
      return { ...state, endTime: action.value };
  }
}

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
  const [s, dispatch] = useReducer(pickerReducer, undefined, (): PickerState => ({
    viewMonth: rangeEnd ?? new Date(),
    pendingStart: rangeStart,
    pendingEnd: rangeEnd,
    startTime: rangeStart ? format(rangeStart, "HH:mm") : "00:00",
    endTime: rangeEnd ? format(rangeEnd, "HH:mm") : "23:59",
    picking: "start",
    prevRange: { start: rangeStart, end: rangeEnd },
  }));

  // Sync from parent when presets or histogram brush update the range.
  if (rangeStart !== s.prevRange.start || rangeEnd !== s.prevRange.end) {
    dispatch({ type: "sync", rangeStart, rangeEnd });
  }

  const handleApply = () => {
    if (!s.pendingStart || !s.pendingEnd) return;
    const [sh = 0, sm = 0] = s.startTime.split(":").map(Number);
    const [eh = 0, em = 0] = s.endTime.split(":").map(Number);
    const start = new Date(s.pendingStart);
    start.setHours(sh, sm, 0, 0);
    const end = new Date(s.pendingEnd);
    end.setHours(eh, em, 59, 999);
    onApply(start, end);
  };

  // Calendar grid
  const monthStart = startOfMonth(s.viewMonth);
  const monthEnd = endOfMonth(s.viewMonth);
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
                ? "bg-copper text-text-on-copper"
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
            className={`text-[0.7em] w-8 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            From
          </span>
          <span
            className={`flex-1 text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {s.pendingStart ? format(s.pendingStart, "yyyy-MM-dd") : "\u2014"}
          </span>
          <input
            type="text"
            value={s.startTime}
            onChange={(e) => {
              const v = e.target.value.replace(/[^0-9:]/g, "");
              if (v.length <= 5) dispatch({ type: "setStartTime", value: v });
            }}
            placeholder="HH:mm"
            aria-label="Start time"
            className={`text-[0.75em] font-mono w-14 px-1 py-0.5 rounded border text-center ${c(
              "bg-ink-surface border-ink-border text-text-normal",
              "bg-light-surface border-light-border text-light-text-normal",
            )}`}
          />
        </div>
        <div className="flex items-center gap-1.5">
          <span
            className={`text-[0.7em] w-8 ${c("text-text-muted", "text-light-text-muted")}`}
          >
            To
          </span>
          <span
            className={`flex-1 text-[0.75em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {s.pendingEnd ? format(s.pendingEnd, "yyyy-MM-dd") : "\u2014"}
          </span>
          <input
            type="text"
            value={s.endTime}
            onChange={(e) => {
              const v = e.target.value.replace(/[^0-9:]/g, "");
              if (v.length <= 5) dispatch({ type: "setEndTime", value: v });
            }}
            placeholder="HH:mm"
            aria-label="End time"
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
          onClick={() => dispatch({ type: "prevMonth" })}
          aria-label="Previous month"
          className={`text-[0.8em] px-1 rounded ${c("text-text-muted hover:text-text-muted", "text-light-text-muted hover:text-light-text-muted")}`}
        >
          {"\u25C2"}
        </button>
        <span
          className={`text-[0.75em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {format(s.viewMonth, "MMMM yyyy")}
        </span>
        <button
          onClick={() => dispatch({ type: "nextMonth" })}
          aria-label="Next month"
          className={`text-[0.8em] px-1 rounded ${c("text-text-muted hover:text-text-muted", "text-light-text-muted hover:text-light-text-muted")}`}
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
              className={`text-center text-[0.65em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {d}
            </div>
          ))}
        </div>
        <div className="grid grid-cols-7 gap-px">
          {days.map((day) => {
            const inMonth = isSameMonth(day, s.viewMonth);
            const isToday = isSameDay(day, today);
            const isStart = s.pendingStart && isSameDay(day, s.pendingStart);
            const isEnd = s.pendingEnd && isSameDay(day, s.pendingEnd);
            const inRange =
              s.pendingStart &&
              s.pendingEnd &&
              isWithinInterval(day, {
                start: s.pendingStart,
                end: s.pendingEnd,
              });
            const selected = isStart || isEnd;

            let dayCls: string;
            if (selected) {
              dayCls = "bg-copper text-text-on-copper";
            } else if (inRange) {
              dayCls = c("bg-copper/10 text-text-normal", "bg-copper/10 text-light-text-normal");
            } else if (inMonth) {
              dayCls = c("text-text-muted hover:bg-ink-hover hover:text-text-normal", "text-light-text-muted hover:bg-light-hover hover:text-light-text-normal");
            } else {
              dayCls = c("text-text-muted/40", "text-light-text-muted/40");
            }
            const todayCls = isToday && !selected ? " underline decoration-copper" : "";

            return (
              <button
                key={day.toISOString()}
                onClick={() => dispatch({ type: "pickDay", day })}
                className={`text-center text-[0.7em] font-mono py-0.5 rounded transition-colors ${dayCls}${todayCls}`}
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
        disabled={!s.pendingStart || !s.pendingEnd}
        className="w-full py-1 text-[0.8em] font-medium rounded bg-copper text-text-on-copper hover:bg-copper-glow transition-all duration-200 disabled:opacity-30 disabled:cursor-not-allowed"
      >
        Apply
      </button>
    </div>
  );
}
