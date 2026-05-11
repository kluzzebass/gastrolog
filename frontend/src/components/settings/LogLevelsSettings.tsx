import { useEffect, useRef, useState } from "react";
import { useConfig } from "../../api/hooks";
import {
  useLogComponents,
  usePutLogLevels,
  levelLabel,
} from "../../api/hooks/useLogLevels";
import {
  LogLevel,
  LogComponentLevelSource,
  type LogLevelConfig,
  type LogLevelRule,
} from "../../api/gen/gastrolog/v1/system_pb";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { Button } from "./Buttons";

interface Props {
  dark: boolean;
}

interface DraftRule {
  pattern: string;
  level: LogLevel;
}

const LEVEL_OPTIONS = [
  { value: String(LogLevel.DEBUG), label: "debug" },
  { value: String(LogLevel.INFO), label: "info" },
  { value: String(LogLevel.WARN), label: "warn" },
  { value: String(LogLevel.ERROR), label: "error" },
];

export function LogLevelsSettings({ dark }: Props) {
  const c = useThemeClass(dark);
  const { addToast } = useToast();
  const { data: config } = useConfig();
  const { data: components } = useLogComponents();
  const putLogLevels = usePutLogLevels();

  const [rulesExpanded, setRulesExpanded] = useState(true);
  const [componentsExpanded, setComponentsExpanded] = useState(false);

  // highlight is the cross-reference target driven by hovering a
  // Components-table row's Source cell. "default" highlights the
  // default-level selector; any other value is a pattern string that
  // highlights the matching rule input.
  const [highlight, setHighlight] = useState<string | null>(null);

  // Refs to the highlighted controls so we can scroll them into view
  // when hover lands — operators don't have to manually scroll the
  // rules list to see what got highlighted.
  const highlightedRuleRef = useRef<HTMLDivElement | null>(null);
  const defaultSelectorRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (highlight === null) return;
    const target = highlight === "default" ? defaultSelectorRef.current : highlightedRuleRef.current;
    target?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }, [highlight]);

  const liveConfig: LogLevelConfig | undefined = config?.logLevels ?? undefined;
  const liveDefault = liveConfig?.defaultLevel ?? LogLevel.INFO;
  const liveRules = liveConfig?.rules ?? [];

  // Local draft — only dispatched on Save.
  const [draftDefault, setDraftDefault] = useState<LogLevel | null>(null);
  const [draftRules, setDraftRules] = useState<DraftRule[] | null>(null);

  const currentDefault = draftDefault ?? liveDefault;
  const currentRules: DraftRule[] = draftRules ?? liveRules.map((r) => ({
    pattern: r.pattern,
    level: r.level,
  }));

  const dirty = draftDefault !== null || draftRules !== null;

  const updateRule = (idx: number, next: Partial<DraftRule>) => {
    setDraftRules(currentRules.map((r, i) => (i === idx ? { ...r, ...next } : r)));
  };
  const removeRule = (idx: number) => {
    setDraftRules(currentRules.filter((_, i) => i !== idx));
  };
  const addRule = () => {
    setDraftRules([...currentRules, { pattern: "", level: LogLevel.DEBUG }]);
  };
  const resetDraft = () => {
    setDraftDefault(null);
    setDraftRules(null);
  };

  // addRuleForPath creates a rule for a specific component path (called
  // from a Components-table click). Auto-expands the Rules card and
  // either focuses an existing rule for that path or appends a new
  // draft rule at DEBUG level (the most common reason an operator
  // would click a path is to lift its verbosity).
  const addRuleForPath = (path: string) => {
    setRulesExpanded(true);
    const existing = currentRules.findIndex((r) => r.pattern === path);
    if (existing >= 0) {
      // Already present — leave it; the operator can edit its level.
      return;
    }
    setDraftRules([...currentRules, { pattern: path, level: LogLevel.DEBUG }]);
  };

  const save = async () => {
    const rules: LogLevelRule[] = currentRules
      .filter((r) => r.pattern.trim() !== "")
      .map((r) => ({ pattern: r.pattern.trim(), level: r.level } as LogLevelRule));
    try {
      await putLogLevels.mutateAsync({
        defaultLevel: currentDefault,
        rules,
      } as LogLevelConfig);
      resetDraft();
      addToast("Log levels updated", "info");
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      addToast(`Update failed: ${msg}`, "error");
    }
  };

  // ruleErrors flags rows that the server would reject — duplicate
  // patterns and invalid syntax. Empty patterns are not flagged because
  // save() filters them out (the operator might be mid-edit on a new
  // row); but they can't satisfy a save either, so the Save button
  // disables when nothing valid remains.
  const ruleErrors = validateRules(currentRules);
  const hasErrors = Object.keys(ruleErrors).length > 0;
  const hasAnyNonEmpty = currentRules.some((r) => r.pattern.trim() !== "");

  const columnHeader = `text-[0.75em] uppercase tracking-wide ${c("text-text-muted", "text-light-text-muted")}`;
  const emptyLine = `text-[0.85em] italic ${c("text-text-muted", "text-light-text-muted")}`;

  return (
    <div className="flex flex-col gap-3">
      <SettingsCard
        id="Rules"
        dark={dark}
        expanded={rulesExpanded}
        onToggle={() => setRulesExpanded(!rulesExpanded)}
        footer={
          <>
            {dirty && (
              <Button
                variant="ghost"
                onClick={resetDraft}
                disabled={putLogLevels.isPending}
                dark={dark}
              >
                Discard
              </Button>
            )}
            <Button
              onClick={save}
              disabled={!dirty || putLogLevels.isPending || hasErrors || (currentRules.length > 0 && !hasAnyNonEmpty)}
              dark={dark}
            >
              {putLogLevels.isPending ? "Saving..." : "Save"}
            </Button>
          </>
        }
      >
        <p className={`text-[0.85em] mb-4 ${c("text-text-muted", "text-light-text-muted")}`}>
          Rules propagate to every node via Raft. Patterns are
          dot-separated; <code>*</code> matches one segment,{" "}
          <code>**</code> matches any depth. Most-specific match wins.
        </p>

        <div
          ref={defaultSelectorRef}
          className={`mb-4 max-w-[260px] rounded transition-colors ${
            highlight === "default" ? "bg-copper/15" : ""
          }`}
        >
          <FormField dark={dark} label="Default level">
            <SelectInput
              dark={dark}
              value={String(currentDefault)}
              onChange={(v) => setDraftDefault(Number(v) as LogLevel)}
              options={LEVEL_OPTIONS}
            />
          </FormField>
        </div>

        <div className="flex flex-col gap-2">
          <div className={`grid grid-cols-[1fr_140px_40px] gap-2 ${columnHeader}`}>
            <div>Pattern</div>
            <div>Level</div>
            <div></div>
          </div>
          {currentRules.length === 0 && (
            <div className={emptyLine}>
              No overrides — every component falls back to the default.
            </div>
          )}
          <div className="max-h-96 overflow-y-auto app-scroll pr-2 flex flex-col gap-2">
            {currentRules.map((r, idx) => {
              const isHighlighted = highlight !== null && highlight !== "default" && r.pattern === highlight;
              return (
                <div
                  key={idx}
                  ref={(el) => {
                    if (isHighlighted) highlightedRuleRef.current = el;
                  }}
                  className={`grid grid-cols-[1fr_140px_40px] gap-2 items-start rounded transition-colors ${
                    isHighlighted ? "bg-copper/15" : ""
                  }`}
                >
                  <div className="flex flex-col gap-1">
                    <TextInput
                      dark={dark}
                      value={r.pattern}
                      onChange={(v) => updateRule(idx, { pattern: v })}
                      error={!!ruleErrors[idx]}
                      title={ruleErrors[idx]}
                      examples={[
                        "orchestrator",
                        "orchestrator.*",
                        "orchestrator.**",
                        "ingester.**.conn",
                      ]}
                    />
                  </div>
                  <SelectInput
                    dark={dark}
                    value={String(r.level)}
                    onChange={(v) => updateRule(idx, { level: Number(v) as LogLevel })}
                    options={LEVEL_OPTIONS}
                  />
                  <button
                    type="button"
                    onClick={() => removeRule(idx)}
                    className={`px-2 py-1 rounded ${c("text-text-muted hover:text-copper hover:bg-ink-hover", "text-light-text-muted hover:text-copper hover:bg-light-hover")}`}
                    aria-label="Remove rule"
                    title="Remove rule"
                  >
                    ×
                  </button>
                </div>
              );
            })}
          </div>
          <div className="mt-2">
            <Button dark={dark} onClick={addRule} variant="ghost">
              + Add rule
            </Button>
          </div>
        </div>
      </SettingsCard>

      <SettingsCard
        id="Components"
        dark={dark}
        expanded={componentsExpanded}
        onToggle={() => setComponentsExpanded(!componentsExpanded)}
      >
        <p className={`text-[0.85em] mb-4 ${c("text-text-muted", "text-light-text-muted")}`}>
          Every component path the binary registers, with the effective level
          and the rule that produced it.
        </p>
        <ComponentsTable
          dark={dark}
          components={components}
          onPathClick={addRuleForPath}
          existingPatterns={new Set(currentRules.map((r) => r.pattern))}
          onSourceHover={(target) => {
            setHighlight(target);
            if (target !== null) setRulesExpanded(true);
          }}
        />
      </SettingsCard>
    </div>
  );
}

interface ComponentsTableProps {
  dark: boolean;
  components: ReturnType<typeof useLogComponents>["data"];
  onPathClick: (path: string) => void;
  existingPatterns: Set<string>;
  // onSourceHover fires when the mouse enters/leaves a row's Source
  // cell. Target is "default" for the default-fallback case, the
  // matching pattern string for an exact/glob rule, or null on leave.
  onSourceHover: (target: string | null) => void;
}

function ComponentsTable({ dark, components, onPathClick, existingPatterns, onSourceHover }: ComponentsTableProps) {
  const c = useThemeClass(dark);
  if (!components || components.length === 0) {
    return (
      <div className={`text-[0.85em] italic ${c("text-text-muted", "text-light-text-muted")}`}>
        No components registered yet.
      </div>
    );
  }
  const header = `text-[0.75em] uppercase tracking-wide ${c("text-text-muted", "text-light-text-muted")}`;
  const cellMono = `text-[0.85em] font-mono ${c("text-text-normal", "text-light-text-normal")}`;
  const cellMeta = `text-[0.85em] font-mono ${c("text-text-muted", "text-light-text-muted")}`;
  const cellDesc = `text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`;
  // max-h caps how much vertical space the registry can claim so the
  // Rules editor stays visible above it. Internal scrolling lets the
  // operator browse the full list without pushing the editor off
  // screen. 24rem is roughly 15 rows at the current row height.
  return (
    <div className="max-h-96 overflow-y-auto app-scroll pr-2">
      <div className="grid grid-cols-[minmax(0,1.4fr)_80px_80px_minmax(0,2fr)] gap-x-4 gap-y-1.5 items-baseline">
        <div className={header}>Path</div>
        <div className={header}>Level</div>
        <div className={header}>Source</div>
        <div className={header}>Description</div>
        {components.map((info) => (
          <FragmentRow
            key={info.path}
            dark={dark}
            pathCls={cellMono}
            levelCls={cellMono}
            sourceCls={cellMeta}
            descCls={cellDesc}
            info={info}
            alreadyRuled={existingPatterns.has(info.path)}
            onPathClick={onPathClick}
            onSourceHover={onSourceHover}
          />
        ))}
      </div>
    </div>
  );
}

function FragmentRow({
  dark,
  pathCls,
  levelCls,
  sourceCls,
  descCls,
  info,
  alreadyRuled,
  onPathClick,
  onSourceHover,
}: {
  dark: boolean;
  pathCls: string;
  levelCls: string;
  sourceCls: string;
  descCls: string;
  info: NonNullable<ReturnType<typeof useLogComponents>["data"]>[number];
  alreadyRuled: boolean;
  onPathClick: (path: string) => void;
  onSourceHover: (target: string | null) => void;
}) {
  const c = useThemeClass(dark);
  const linkCls = alreadyRuled
    ? c("text-copper-dim cursor-default", "text-copper-dim cursor-default")
    : c(
        "hover:text-copper hover:underline cursor-pointer",
        "hover:text-copper hover:underline cursor-pointer",
      );

  // hoverTarget: what to highlight in the Rules editor when this row's
  // Source cell is hovered. DEFAULT → "default" (the fallback selector);
  // any rule match → the winning pattern string.
  const hoverTarget =
    info.source === LogComponentLevelSource.LOG_LEVEL_SOURCE_DEFAULT
      ? "default"
      : info.matchingPattern;

  return (
    <>
      <button
        type="button"
        onClick={() => { if (!alreadyRuled) onPathClick(info.path); }}
        disabled={alreadyRuled}
        title={alreadyRuled ? "rule already exists for this path" : "click to add a rule for this path"}
        className={`${pathCls} break-all text-left ${linkCls}`}
      >
        {info.path}
      </button>
      <div className={levelCls}>{levelLabel(info.effectiveLevel)}</div>
      <div
        className={`${sourceCls} cursor-help ${c("hover:text-copper", "hover:text-copper")}`}
        onMouseEnter={() => onSourceHover(hoverTarget)}
        onMouseLeave={() => onSourceHover(null)}
        title={
          info.source === LogComponentLevelSource.LOG_LEVEL_SOURCE_DEFAULT
            ? "no rule matched — falling back to the default level"
            : `matched by rule: ${info.matchingPattern}`
        }
      >
        {sourceLabel(info.source)}
      </div>
      <div className={descCls}>{info.description || "—"}</div>
    </>
  );
}

// validateRules mirrors the server-side validation in
// internal/server/system_log_levels.go: rejects duplicate patterns and
// patterns that don't parse under the logging.ValidatePattern grammar
// (literal segments [a-z0-9_-]+, "*" for one segment, "**" for any
// depth). Empty patterns are NOT flagged here — they're silently
// filtered on save, since they typically mean "the operator clicked
// Add rule and hasn't typed yet." The Save button separately disables
// when no non-empty row exists.
function validateRules(rules: DraftRule[]): Record<number, string> {
  const errors: Record<number, string> = {};
  const seen = new Map<string, number>();
  for (const [idx, r] of rules.entries()) {
    const p = r.pattern.trim();
    if (p === "") continue;
    if (!isValidPattern(p)) {
      errors[idx] = "Invalid pattern. Use lowercase a–z, 0–9, hyphen, underscore; '*' for one segment; '**' for any depth.";
      continue;
    }
    const prev = seen.get(p);
    if (prev !== undefined) {
      errors[idx] = "Duplicate pattern";
      errors[prev] = "Duplicate pattern";
      continue;
    }
    seen.set(p, idx);
  }
  return errors;
}

function isValidPattern(p: string): boolean {
  if (p === "") return false;
  for (const seg of p.split(".")) {
    if (seg === "") return false;
    if (seg === "*" || seg === "**") continue;
    if (!/^[a-z0-9_-]+$/.test(seg)) return false;
  }
  return true;
}

function sourceLabel(s: LogComponentLevelSource): string {
  switch (s) {
    case LogComponentLevelSource.LOG_LEVEL_SOURCE_DEFAULT: return "default";
    case LogComponentLevelSource.LOG_LEVEL_SOURCE_EXACT_RULE: return "exact";
    case LogComponentLevelSource.LOG_LEVEL_SOURCE_GLOB_RULE: return "glob";
    default: return "";
  }
}
