import { useState } from "react";
import { ChunkPlan, BranchPlan, PipelineStep } from "../api/client";
import { formatChunkId } from "../utils";

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
  const c = (d: string, l: string) => (dark ? d : l);
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
        {expression && <ExpressionBox expression={expression} dark={dark} />}
      </div>

      {/* Scrollable chunk list */}
      <div className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden app-scroll">
        <div className="flex flex-col gap-2 stagger-children">
          {chunks.map((plan, i) => (
            <ExplainChunk
              key={i}
              plan={plan}
              dark={dark}
              collapsed={!!collapsed[i]}
              onToggle={() => toggle(i)}
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
}: {
  expression: string;
  dark: boolean;
}) {
  const c = (d: string, l: string) => (dark ? d : l);

  return (
    <textarea
      readOnly
      value={expression}
      rows={1}
      style={{ fieldSizing: "content" } as React.CSSProperties}
      className={`w-full font-mono text-[0.8em] px-3 py-1.5 rounded mb-3 resize-none overflow-hidden border-none outline-none ${c("bg-ink-surface text-text-normal", "bg-light-surface text-light-text-normal")}`}
    />
  );
}

function ExplainChunk({
  plan,
  dark,
  collapsed,
  onToggle,
}: {
  plan: ChunkPlan;
  dark: boolean;
  collapsed: boolean;
  onToggle: () => void;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
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
          {plan.storeId && (
            <span
              className={`font-mono text-sm ${c("text-text-muted", "text-light-text-muted")}`}
            >
              [{plan.storeId}]
            </span>
          )}
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
                />
              ))}
            </div>
          ) : plan.steps.length > 0 ? (
            <PipelineFunnel
              steps={plan.steps}
              totalRecords={totalRecords}
              dark={dark}
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
}: {
  branch: BranchPlan;
  index: number;
  totalRecords: number;
  dark: boolean;
}) {
  const c = (d: string, l: string) => (dark ? d : l);

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
        />
      )}
    </div>
  );
}

function PipelineFunnel({
  steps,
  totalRecords,
  dark,
}: {
  steps: PipelineStep[];
  totalRecords: number;
  dark: boolean;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
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
          <div key={i} className="relative pb-3 min-w-0">
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
                    title={step.predicate}
                  >
                    {step.predicate}
                  </span>
                )}
                {(step.reason || step.detail) && (
                  <span
                    className={`font-mono text-xs truncate ${c("text-text-muted", "text-light-text-muted")}`}
                    title={step.detail || step.reason}
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
