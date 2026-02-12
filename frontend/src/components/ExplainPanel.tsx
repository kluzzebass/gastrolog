import { useState, useCallback } from "react";
import { useThemeClass } from "../hooks/useThemeClass";
import { ChunkPlan, BranchPlan, PipelineStep } from "../api/client";
import { formatChunkId } from "../utils";

// ── Highlight helpers ──

type Range = [number, number]; // [startIdx, endIdx) in expression string

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Find all non-overlapping matches of a regex in the expression, return char ranges. */
function findRanges(expression: string, re: RegExp): Range[] {
  const ranges: Range[] = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(expression)) !== null) {
    ranges.push([m.index, m.index + m[0].length]);
  }
  return ranges;
}

/** Find a bare word in the expression that isn't part of a key=value token. */
function findBareWordRanges(expression: string, word: string): Range[] {
  const re = new RegExp(`(?<!=)\\b${escapeRegex(word)}\\b(?!=)`, "gi");
  return findRanges(expression, re);
}

/** Map a pipeline step to character ranges in the expression string. */
function stepToRanges(step: PipelineStep, expression: string): Range[] {
  switch (step.name) {
    case "time":
      return [
        ...findRanges(expression, /\bstart=\S+/g),
        ...findRanges(expression, /\bend=\S+/g),
      ];

    case "token": {
      const inner = step.predicate.match(/^token\((.+)\)$/)?.[1];
      if (!inner) return [];
      return inner
        .split(/,\s*/)
        .flatMap((w) => findBareWordRanges(expression, w));
    }

    case "kv": {
      // Predicate is the literal token, e.g. "level=error"
      const re = new RegExp(`\\b${escapeRegex(step.predicate)}\\b`, "gi");
      return findRanges(expression, re);
    }

    default:
      return [];
  }
}

/** Split expression into segments: [{text, highlighted}] based on ranges. */
function buildSegments(
  expression: string,
  ranges: Range[],
): { text: string; highlighted: boolean }[] {
  if (ranges.length === 0) return [{ text: expression, highlighted: false }];

  // Sort and merge overlapping ranges.
  const sorted = [...ranges].sort((a, b) => a[0] - b[0]);
  const merged: Range[] = [sorted[0]!];
  for (let i = 1; i < sorted.length; i++) {
    const prev = merged[merged.length - 1]!;
    if (sorted[i]![0] <= prev[1]) {
      prev[1] = Math.max(prev[1], sorted[i]![1]);
    } else {
      merged.push(sorted[i]!);
    }
  }

  const segments: { text: string; highlighted: boolean }[] = [];
  let cursor = 0;
  for (const [start, end] of merged) {
    if (cursor < start) {
      segments.push({
        text: expression.slice(cursor, start),
        highlighted: false,
      });
    }
    segments.push({ text: expression.slice(start, end), highlighted: true });
    cursor = end;
  }
  if (cursor < expression.length) {
    segments.push({ text: expression.slice(cursor), highlighted: false });
  }
  return segments;
}

export function ExplainPanel({
  chunks,
  direction,
  totalChunks,
  expression,
  dark,
}: {
  chunks: ChunkPlan[];
  direction: string;
  totalChunks: number;
  expression: string;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  const [highlightRanges, setHighlightRanges] = useState<Range[]>([]);

  const handleStepHover = useCallback(
    (step: PipelineStep) => {
      setHighlightRanges(stepToRanges(step, expression));
    },
    [expression],
  );

  const handleStepLeave = useCallback(() => {
    setHighlightRanges([]);
  }, []);

  const [collapsed, setCollapsed] = useState<Record<number, boolean>>(() => {
    // Auto-collapse if more than 3 chunks
    if (chunks.length <= 3) return {};
    const m: Record<number, boolean> = {};
    for (let i = 0; i < chunks.length; i++) m[i] = true;
    return m;
  });

  const toggle = (i: number) =>
    setCollapsed((prev) => ({ ...prev, [i]: !prev[i] }));

  return (
    <div className="flex flex-col min-h-0 h-full">
      {/* Fixed header */}
      <div className="shrink-0">
        <div className="flex items-center gap-3 mb-3">
          <h3
            className={`font-display text-[1.15em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Execution Plan
          </h3>
          <span
            className={`text-[0.65em] px-1.5 py-0.5 rounded uppercase tracking-wider font-semibold ${
              direction === "reverse"
                ? "bg-copper/15 text-copper border border-copper/25"
                : "bg-severity-info/15 text-severity-info border border-severity-info/25"
            }`}
          >
            {direction || "forward"}
          </span>
          <span
            className={`font-mono text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {chunks.length} of {totalChunks} chunks
          </span>
        </div>
        {expression && (
          <ExpressionBox
            expression={expression}
            dark={dark}
            highlightRanges={highlightRanges}
          />
        )}
      </div>

      {/* Cost summary */}
      {chunks.length > 0 && <CostSummary chunks={chunks} dark={dark} />}

      {/* Scrollable chunk list */}
      <div className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden app-scroll">
        <div className="flex flex-col gap-2">
          {chunks.map((plan, i) => (
            <ExplainChunk
              key={i}
              plan={plan}
              dark={dark}
              collapsed={!!collapsed[i]}
              onToggle={() => toggle(i)}
              onStepHover={handleStepHover}
              onStepLeave={handleStepLeave}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function ExpressionBox({
  expression,
  dark,
  highlightRanges,
}: {
  expression: string;
  dark: boolean;
  highlightRanges: Range[];
}) {
  const c = useThemeClass(dark);
  const segments = buildSegments(expression, highlightRanges);

  return (
    <div
      className={`w-full font-mono text-[0.8em] px-3 py-1.5 rounded mb-3 whitespace-pre-wrap break-all ${c("bg-ink-surface text-text-normal", "bg-light-surface text-light-text-normal")}`}
    >
      {segments.map((seg, i) =>
        seg.highlighted ? (
          <mark
            key={i}
            className={`rounded-sm transition-colors duration-150 ${c("bg-copper/25 text-copper", "bg-copper/20 text-copper")}`}
          >
            {seg.text}
          </mark>
        ) : (
          <span key={i}>{seg.text}</span>
        ),
      )}
    </div>
  );
}

function CostSummary({ chunks, dark }: { chunks: ChunkPlan[]; dark: boolean }) {
  const c = useThemeClass(dark);

  const totalRecords = chunks.reduce(
    (sum, ch) => sum + Number(ch.recordCount),
    0,
  );
  const skipped = chunks.filter((ch) => ch.scanMode === "skipped");
  const scanned = chunks.filter((ch) => ch.scanMode !== "skipped");
  const totalScan = scanned.reduce(
    (sum, ch) => sum + Number(ch.estimatedRecords),
    0,
  );
  const indexDriven = scanned.filter(
    (ch) => ch.scanMode === "index-driven",
  ).length;
  const sequential = scanned.filter(
    (ch) => ch.scanMode === "sequential",
  ).length;
  const reduction = totalRecords > 0 ? (1 - totalScan / totalRecords) * 100 : 0;

  return (
    <div
      className={`shrink-0 rounded border px-3.5 py-2.5 mb-3 ${c("bg-ink-surface border-ink-border-subtle", "bg-light-surface border-light-border-subtle")}`}
    >
      <div className="flex flex-wrap items-center gap-x-5 gap-y-1.5 text-[0.8em] font-mono">
        <span className={c("text-text-muted", "text-light-text-muted")}>
          scan{" "}
          <strong className={c("text-text-bright", "text-light-text-bright")}>
            ~{totalScan.toLocaleString()}
          </strong>
          <span className={c("text-text-ghost", "text-light-text-ghost")}>
            {" "}
            / {totalRecords.toLocaleString()} records
          </span>
        </span>
        <span className={c("text-text-muted", "text-light-text-muted")}>
          chunks{" "}
          <strong className={c("text-text-bright", "text-light-text-bright")}>
            {scanned.length}
          </strong>
          {skipped.length > 0 && (
            <span className={c("text-text-ghost", "text-light-text-ghost")}>
              {" "}
              ({skipped.length} skipped)
            </span>
          )}
        </span>
        {scanned.length > 0 && (
          <span className={c("text-text-muted", "text-light-text-muted")}>
            {indexDriven > 0 && (
              <span className="text-severity-info">{indexDriven} indexed</span>
            )}
            {indexDriven > 0 && sequential > 0 && ", "}
            {sequential > 0 && (
              <span className="text-severity-warn">
                {sequential} sequential
              </span>
            )}
          </span>
        )}
        {reduction > 0 && (
          <span className="text-copper font-semibold">
            {reduction.toFixed(0)}% reduced
          </span>
        )}
      </div>
      {/* Reduction bar */}
      {totalRecords > 0 && (
        <div
          className={`mt-2 h-1.5 rounded-full overflow-hidden ${c("bg-ink-border-subtle/60", "bg-light-border/50")}`}
        >
          <div
            className="h-full bg-copper/80 rounded-full"
            style={{
              width: `${Math.min(Math.max((totalScan / totalRecords) * 100, 0.5), 100)}%`,
            }}
          />
        </div>
      )}
    </div>
  );
}

function ExplainChunk({
  plan,
  dark,
  collapsed,
  onToggle,
  onStepHover,
  onStepLeave,
}: {
  plan: ChunkPlan;
  dark: boolean;
  collapsed: boolean;
  onToggle: () => void;
  onStepHover: (step: PipelineStep) => void;
  onStepLeave: () => void;
}) {
  const c = useThemeClass(dark);
  const isSkipped = plan.scanMode === "skipped";
  const hasBranches = plan.branchPlans.length > 0;
  const totalRecords = Number(plan.recordCount);

  const formatTs = (ts: { toDate(): Date } | undefined) => {
    if (!ts) return "";
    return ts.toDate().toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  };

  return (
    <div
      className={`rounded border overflow-hidden ${
        isSkipped
          ? c(
              "bg-ink-surface/50 border-ink-border-subtle/50",
              "bg-light-surface/50 border-light-border-subtle/50",
            )
          : c(
              "bg-ink-surface border-ink-border-subtle",
              "bg-light-surface border-light-border-subtle",
            )
      }`}
    >
      {/* Chunk header — clickable to toggle */}
      <button
        onClick={onToggle}
        className={`w-full min-w-0 relative px-3.5 pt-2.5 pb-4 text-left transition-colors ${
          !collapsed
            ? c(
                "border-b border-ink-border-subtle",
                "border-b border-light-border-subtle",
              )
            : ""
        }`}
      >
        <div className="flex items-center gap-2 w-full min-w-0">
          <span
            className={`text-xs transition-transform ${collapsed ? "" : "rotate-90"} ${c("text-text-muted", "text-light-text-muted")}`}
          >
            &#x25B6;
          </span>
          {plan.storeId && (
            <span
              className={`text-xs px-1.5 py-0.5 rounded font-medium ${c("bg-ink-border-subtle/40 text-text-normal", "bg-light-border/40 text-light-text-normal")}`}
              title={`Store: ${plan.storeId}`}
            >
              {plan.storeId}
            </span>
          )}
          <span
            className={`font-mono text-sm font-medium ${
              isSkipped
                ? c("text-text-muted", "text-light-text-muted")
                : c("text-text-bright", "text-light-text-bright")
            }`}
          >
            {formatChunkId(plan.chunkId)}
          </span>
          <span
            className={`text-xs px-1.5 py-0.5 rounded uppercase tracking-wider font-semibold ${
              isSkipped
                ? "bg-severity-error/15 text-severity-error border border-severity-error/25"
                : plan.sealed
                  ? "bg-severity-info/15 text-severity-info border border-severity-info/25"
                  : "bg-copper/15 text-copper border border-copper/25"
            }`}
          >
            {isSkipped ? "Skip" : plan.sealed ? "Sealed" : "Active"}
          </span>
          <span
            className={`font-mono text-sm ${c("text-text-normal", "text-light-text-normal")}`}
          >
            {totalRecords.toLocaleString()} rec
          </span>
          {(plan.startTs || plan.endTs) && (
            <span
              className={`font-mono text-xs ${c("text-text-muted", "text-light-text-muted")}`}
            >
              {formatTs(plan.startTs)}
              {plan.startTs && plan.endTs ? " \u2013 " : ""}
              {formatTs(plan.endTs)}
            </span>
          )}
          {isSkipped && plan.skipReason && (
            <span
              className={`ml-auto font-mono text-sm ${c("text-severity-error/80", "text-severity-error/90")}`}
            >
              {plan.skipReason}
            </span>
          )}
          {!isSkipped && (
            <span
              className={`ml-auto font-mono text-sm ${c("text-text-normal", "text-light-text-normal")}`}
            >
              scan ~{Number(plan.estimatedRecords).toLocaleString()}
            </span>
          )}
        </div>
        {/* Aggregate narrowing bar */}
        {!isSkipped && totalRecords > 0 && (
          <div className="absolute bottom-0 left-3.5 right-3.5 h-1.5 rounded-full overflow-hidden">
            <div
              className={`absolute inset-0 ${c("bg-ink-border-subtle/60", "bg-light-border/50")}`}
            />
            <div
              className="absolute inset-y-0 left-0 bg-copper/80"
              style={{
                width: `${Math.min(Math.max((Number(plan.estimatedRecords) / totalRecords) * 100, 0.5), 100)}%`,
              }}
            />
          </div>
        )}
      </button>

      {/* Chunk body — pipeline */}
      {!collapsed && !isSkipped && (
        <div className="px-3.5 py-4">
          {hasBranches ? (
            <div className="flex flex-col gap-4">
              {plan.branchPlans.map((bp, j) => (
                <ExplainBranch
                  key={j}
                  branch={bp}
                  index={j}
                  totalRecords={totalRecords}
                  dark={dark}
                  onStepHover={onStepHover}
                  onStepLeave={onStepLeave}
                />
              ))}
            </div>
          ) : plan.steps.length > 0 ? (
            <PipelineFunnel
              steps={plan.steps}
              totalRecords={totalRecords}
              dark={dark}
              onStepHover={onStepHover}
              onStepLeave={onStepLeave}
            />
          ) : null}

          {/* Footer */}
          <div
            className={`flex flex-wrap items-center gap-x-4 gap-y-1.5 mt-4 pt-3 text-xs font-mono border-t ${c("border-ink-border-subtle text-text-normal", "border-light-border-subtle text-light-text-normal")}`}
          >
            <span>
              scan{" "}
              <strong
                className={c("text-text-bright", "text-light-text-bright")}
              >
                {plan.scanMode}
              </strong>
            </span>
            <span>
              records to scan{" "}
              <strong
                className={c("text-text-bright", "text-light-text-bright")}
              >
                ~{Number(plan.estimatedRecords).toLocaleString()}
              </strong>
            </span>
            {plan.runtimeFilters
              .filter((f) => f && f !== "none")
              .map((f, i) => (
                <span
                  key={i}
                  className={`px-1.5 py-px rounded ${c("bg-severity-warn/10 text-severity-warn", "bg-severity-warn/10 text-severity-warn")}`}
                >
                  {f}
                </span>
              ))}
          </div>
        </div>
      )}
    </div>
  );
}

function ExplainBranch({
  branch,
  index,
  totalRecords,
  dark,
  onStepHover,
  onStepLeave,
}: {
  branch: BranchPlan;
  index: number;
  totalRecords: number;
  dark: boolean;
  onStepHover: (step: PipelineStep) => void;
  onStepLeave: () => void;
}) {
  const c = useThemeClass(dark);

  return (
    <div
      className={`rounded border px-3.5 py-3 ${
        branch.skipped
          ? c(
              "bg-ink/30 border-ink-border-subtle/50",
              "bg-light-bg/50 border-light-border-subtle/50",
            )
          : c(
              "bg-ink border-ink-border-subtle",
              "bg-light-bg border-light-border-subtle",
            )
      }`}
    >
      <div className="flex items-center gap-2.5 mb-4">
        <span
          className={`text-xs font-medium uppercase tracking-wider ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Branch {index + 1}
        </span>
        <span
          className={`font-mono text-sm ${
            branch.skipped
              ? c("text-text-muted", "text-light-text-muted")
              : c("text-text-bright", "text-light-text-bright")
          }`}
        >
          {branch.expression}
        </span>
        {branch.skipped && (
          <span className="text-xs px-1.5 py-0.5 rounded uppercase tracking-wider font-semibold bg-severity-error/15 text-severity-error border border-severity-error/25">
            Skip
          </span>
        )}
        {branch.skipped && branch.skipReason && (
          <span
            className={`font-mono text-sm ${c("text-severity-error/80", "text-severity-error/90")}`}
          >
            {branch.skipReason}
          </span>
        )}
        {!branch.skipped && (
          <span
            className={`ml-auto font-mono text-sm ${c("text-text-normal", "text-light-text-normal")}`}
          >
            scan ~{Number(branch.estimatedRecords).toLocaleString()}
          </span>
        )}
      </div>
      {!branch.skipped && branch.steps.length > 0 && (
        <PipelineFunnel
          steps={branch.steps}
          totalRecords={totalRecords}
          dark={dark}
          onStepHover={onStepHover}
          onStepLeave={onStepLeave}
        />
      )}
    </div>
  );
}

function PipelineFunnel({
  steps,
  totalRecords,
  dark,
  onStepHover,
  onStepLeave,
}: {
  steps: PipelineStep[];
  totalRecords: number;
  dark: boolean;
  onStepHover?: (step: PipelineStep) => void;
  onStepLeave?: () => void;
}) {
  const c = useThemeClass(dark);
  const maxVal = Math.max(
    totalRecords,
    ...steps.map((s) => Number(s.inputEstimate)),
    1,
  );

  const actionColor = (action: string) => {
    switch (action) {
      case "seek":
      case "indexed":
        return "bg-severity-info/20 text-severity-info border-severity-info/30";
      case "skipped":
        return "bg-severity-error/15 text-severity-error border-severity-error/25";
      default:
        return "bg-severity-warn/15 text-severity-warn border-severity-warn/25";
    }
  };

  return (
    <div className="flex flex-col gap-4">
      {steps.map((step, i) => {
        const inVal = Number(step.inputEstimate);
        const outVal = Number(step.outputEstimate);
        const inPct = maxVal > 0 ? (inVal / maxVal) * 100 : 0;
        const outPct = maxVal > 0 ? (outVal / maxVal) * 100 : 0;
        const reduced = inVal > 0 && outVal < inVal;

        return (
          <div
            key={i}
            className="relative pb-3 min-w-0"
            onMouseEnter={() => onStepHover?.(step)}
            onMouseLeave={() => onStepLeave?.()}
          >
            {/* Top row: step info */}
            <div className="flex items-center gap-3 min-w-0">
              {/* Step number */}
              <span
                className={`w-5 text-right text-xs font-mono ${c("text-text-muted", "text-light-text-muted")}`}
              >
                {i + 1}
              </span>

              {/* Name */}
              <span
                className={`text-sm font-semibold capitalize ${c("text-text-bright", "text-light-text-bright")}`}
              >
                {step.name}
              </span>

              {/* Action badge */}
              <span
                className={`text-xs px-1.5 py-0.5 rounded border uppercase tracking-wide font-semibold ${actionColor(step.action)}`}
              >
                {step.action}
              </span>

              {/* Counts */}
              <span
                className={`font-mono text-sm ${c("text-text-normal", "text-light-text-normal")}`}
              >
                <span className={c("text-text-muted", "text-light-text-muted")}>
                  candidates{" "}
                </span>
                {inVal.toLocaleString()}
                {" \u2192 "}
                <span
                  className={
                    reduced
                      ? "text-copper font-semibold"
                      : c("text-text-normal", "text-light-text-normal")
                  }
                >
                  {outVal.toLocaleString()}
                </span>
              </span>

              {/* Predicate + reason */}
              <span className="ml-auto flex items-center gap-2 min-w-0 max-w-[60%]">
                {step.predicate && (
                  <span
                    className={`font-mono text-xs truncate ${c("text-text-normal", "text-light-text-normal")}`}
                  >
                    {step.predicate}
                  </span>
                )}
                {(step.reason || step.detail) && (
                  <span
                    className={`font-mono text-xs truncate ${c("text-text-muted", "text-light-text-muted")}`}
                  >
                    {step.detail || step.reason}
                  </span>
                )}
              </span>
            </div>

            {/* Narrowing bar */}
            <div className="absolute bottom-0 left-8 right-0 h-1.5 rounded-full overflow-hidden">
              <div
                className={`absolute inset-0 ${c("bg-ink-border-subtle/60", "bg-light-border/50")}`}
                style={{ width: `${Math.min(Math.max(inPct, 1), 100)}%` }}
              />
              <div
                className="absolute inset-y-0 left-0 bg-copper/80"
                style={{ width: `${Math.min(Math.max(outPct, 0.5), 100)}%` }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
