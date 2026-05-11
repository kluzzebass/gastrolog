import { useState } from "react";
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
import { FormField, TextInput } from "./FormField";
import { Button } from "./Buttons";

interface Props {
  dark: boolean;
}

interface DraftRule {
  pattern: string;
  level: LogLevel;
}

const LEVEL_CHOICES: { value: LogLevel; label: string }[] = [
  { value: LogLevel.DEBUG, label: "debug" },
  { value: LogLevel.INFO, label: "info" },
  { value: LogLevel.WARN, label: "warn" },
  { value: LogLevel.ERROR, label: "error" },
];

export function LogLevelsSettings({ dark }: Props) {
  const c = useThemeClass(dark);
  const { addToast } = useToast();
  const { data: config } = useConfig();
  const { data: components } = useLogComponents();
  const putLogLevels = usePutLogLevels();

  const liveConfig: LogLevelConfig | undefined = config?.logLevels ?? undefined;
  const liveDefault = liveConfig?.defaultLevel ?? LogLevel.INFO;
  const liveRules = liveConfig?.rules ?? [];

  // Local draft state — only dispatched on Save.
  const [draftDefault, setDraftDefault] = useState<LogLevel | null>(null);
  const [draftRules, setDraftRules] = useState<DraftRule[] | null>(null);

  const currentDefault = draftDefault ?? liveDefault;
  const currentRules: DraftRule[] = draftRules ?? liveRules.map((r) => ({
    pattern: r.pattern,
    level: r.level,
  }));

  const dirty = draftDefault !== null || draftRules !== null;

  const updateRule = (idx: number, next: Partial<DraftRule>) => {
    const rules = currentRules.map((r, i) => (i === idx ? { ...r, ...next } : r));
    setDraftRules(rules);
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
      addToast("Log levels updated");
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      addToast(`Update failed: ${msg}`, "error");
    }
  };

  return (
    <div className="flex flex-col gap-8">
      <section>
        <h3 className={`mb-2 text-[1.1em] font-display ${c("text-text-bright", "text-light-text-bright")}`}>Log Levels</h3>
        <p className={`mb-4 text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
          Per-component log levels propagate to every node via Raft. Patterns
          are dot-separated; <code>*</code> matches one segment,{" "}
          <code>**</code> matches any depth.
        </p>

        <div className="mb-4">
          <FormField dark={dark} label="Default level">
            <LevelSelect
              dark={dark}
              value={currentDefault}
              onChange={(v) => setDraftDefault(v)}
            />
          </FormField>
        </div>

        <div className="flex flex-col gap-2">
          <div className={`grid grid-cols-[1fr_140px_40px] gap-2 text-[0.75em] uppercase tracking-wide ${c("text-text-ghost", "text-light-text-ghost")}`}>
            <div>Pattern</div>
            <div>Level</div>
            <div></div>
          </div>
          {currentRules.length === 0 && (
            <div className={`text-[0.85em] italic ${c("text-text-ghost", "text-light-text-ghost")}`}>
              No overrides — every component falls back to the default.
            </div>
          )}
          {currentRules.map((r, idx) => (
            <div key={idx} className="grid grid-cols-[1fr_140px_40px] gap-2 items-center">
              <TextInput
                dark={dark}
                value={r.pattern}
                onChange={(v) => updateRule(idx, { pattern: v })}
                placeholder="orchestrator.replication.**"
              />
              <LevelSelect
                dark={dark}
                value={r.level}
                onChange={(v) => updateRule(idx, { level: v })}
              />
              <button
                type="button"
                onClick={() => removeRule(idx)}
                className={`px-2 py-1 ${c("text-text-muted hover:text-copper", "text-light-text-muted hover:text-copper")}`}
                aria-label="Remove rule"
                title="Remove rule"
              >
                ×
              </button>
            </div>
          ))}
          <div className="mt-2">
            <Button dark={dark} onClick={addRule} variant="ghost">
              + Add rule
            </Button>
          </div>
        </div>

        {dirty && (
          <div className="mt-6 flex gap-2 justify-end">
            <Button dark={dark} onClick={resetDraft} variant="ghost">
              Discard
            </Button>
            <Button dark={dark} onClick={save} variant="primary" disabled={putLogLevels.isPending}>
              {putLogLevels.isPending ? "Saving…" : "Save changes"}
            </Button>
          </div>
        )}
      </section>

      <section>
        <h3 className={`mb-2 text-[1.1em] font-display ${c("text-text-bright", "text-light-text-bright")}`}>Components</h3>
        <p className={`mb-4 text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
          Every component path the binary registers, with the effective level
          and the rule that produced it.
        </p>
        <ComponentsTable dark={dark} components={components} />
      </section>
    </div>
  );
}

interface LevelSelectProps {
  dark: boolean;
  value: LogLevel;
  onChange: (v: LogLevel) => void;
}

function LevelSelect({ dark, value, onChange }: LevelSelectProps) {
  const c = useThemeClass(dark);
  return (
    <select
      value={value}
      onChange={(e) => onChange(Number(e.target.value) as LogLevel)}
      className={`px-2 py-1 rounded text-[0.85em] ${c("bg-surface text-text-normal border border-border", "bg-light-surface text-light-text-normal border border-light-border")}`}
    >
      {LEVEL_CHOICES.map((opt) => (
        <option key={opt.value} value={opt.value}>{opt.label}</option>
      ))}
    </select>
  );
}

interface ComponentsTableProps {
  dark: boolean;
  components: ReturnType<typeof useLogComponents>["data"];
}

function ComponentsTable({ dark, components }: ComponentsTableProps) {
  const c = useThemeClass(dark);
  if (!components || components.length === 0) {
    return (
      <div className={`text-[0.85em] italic ${c("text-text-ghost", "text-light-text-ghost")}`}>
        No components registered yet.
      </div>
    );
  }
  return (
    <div className="grid grid-cols-[1fr_100px_100px] gap-x-4 gap-y-1 text-[0.85em] font-mono">
      <div className={`text-[0.75em] uppercase tracking-wide ${c("text-text-ghost", "text-light-text-ghost")}`}>Path</div>
      <div className={`text-[0.75em] uppercase tracking-wide ${c("text-text-ghost", "text-light-text-ghost")}`}>Level</div>
      <div className={`text-[0.75em] uppercase tracking-wide ${c("text-text-ghost", "text-light-text-ghost")}`}>Source</div>
      {components.map((info) => (
        <Row key={info.path} dark={dark} info={info} />
      ))}
    </div>
  );
}

function Row({
  dark,
  info,
}: {
  dark: boolean;
  info: NonNullable<ReturnType<typeof useLogComponents>["data"]>[number];
}) {
  const c = useThemeClass(dark);
  return (
    <>
      <div className={c("text-text-normal", "text-light-text-normal")}>{info.path}</div>
      <div className={c("text-text-normal", "text-light-text-normal")}>{levelLabel(info.effectiveLevel)}</div>
      <div className={c("text-text-muted", "text-light-text-muted")}>{sourceLabel(info.source)}</div>
    </>
  );
}

function sourceLabel(s: LogComponentLevelSource): string {
  switch (s) {
    case LogComponentLevelSource.LOG_LEVEL_SOURCE_DEFAULT: return "default";
    case LogComponentLevelSource.LOG_LEVEL_SOURCE_EXACT_RULE: return "exact";
    case LogComponentLevelSource.LOG_LEVEL_SOURCE_GLOB_RULE: return "glob";
    default: return "";
  }
}
