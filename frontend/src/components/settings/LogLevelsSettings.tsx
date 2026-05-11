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
              disabled={!dirty || putLogLevels.isPending}
              dark={dark}
            >
              {putLogLevels.isPending ? "Saving..." : "Save"}
            </Button>
          </>
        }
      >
        <p className={`text-[0.85em] mb-4 ${c("text-text-muted", "text-light-text-muted")}`}>
          Per-component log levels propagate to every node via Raft. Patterns
          are dot-separated; <code>*</code> matches one segment,{" "}
          <code>**</code> matches any depth.
        </p>

        <div className="mb-4 max-w-[260px]">
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
          {currentRules.map((r, idx) => (
            <div key={idx} className="grid grid-cols-[1fr_140px_40px] gap-2 items-center">
              <TextInput
                dark={dark}
                value={r.pattern}
                onChange={(v) => updateRule(idx, { pattern: v })}
                placeholder="orchestrator.replication.**"
              />
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
          ))}
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
        <ComponentsTable dark={dark} components={components} />
      </SettingsCard>
    </div>
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
      <div className={`text-[0.85em] italic ${c("text-text-muted", "text-light-text-muted")}`}>
        No components registered yet.
      </div>
    );
  }
  const header = `text-[0.75em] uppercase tracking-wide ${c("text-text-muted", "text-light-text-muted")}`;
  const cell = `text-[0.85em] font-mono ${c("text-text-normal", "text-light-text-normal")}`;
  const meta = `text-[0.85em] font-mono ${c("text-text-muted", "text-light-text-muted")}`;
  return (
    <div className="grid grid-cols-[1fr_100px_100px] gap-x-4 gap-y-1">
      <div className={header}>Path</div>
      <div className={header}>Level</div>
      <div className={header}>Source</div>
      {components.map((info) => (
        <FragmentRow
          key={info.path}
          pathCls={cell}
          levelCls={cell}
          sourceCls={meta}
          info={info}
        />
      ))}
    </div>
  );
}

function FragmentRow({
  pathCls,
  levelCls,
  sourceCls,
  info,
}: {
  pathCls: string;
  levelCls: string;
  sourceCls: string;
  info: NonNullable<ReturnType<typeof useLogComponents>["data"]>[number];
}) {
  return (
    <>
      <div className={pathCls}>{info.path}</div>
      <div className={levelCls}>{levelLabel(info.effectiveLevel)}</div>
      <div className={sourceCls}>{sourceLabel(info.source)}</div>
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
