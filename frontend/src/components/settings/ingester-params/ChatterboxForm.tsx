import { FormField, TextInput, NumberInput } from "../FormField";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { Checkbox } from "../Checkbox";
import type { SubFormProps } from "./types";
import {
  ALL_FORMATS,
  parseFormats,
  parseWeights,
  serializeFormats,
  serializeWeights,
} from "./chatterbox-helpers";

export function ChatterboxForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  const c = useThemeClass(dark);
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });
  const get = (key: string) => params[key] ?? "";

  const enabled = parseFormats(get("formats"));
  const weights = parseWeights(get("formatWeights"));

  const toggleFormat = (id: string) => {
    const next = new Set(enabled);
    if (next.has(id)) {
      if (next.size <= 1) return; // must have at least one format
      next.delete(id);
    } else {
      next.add(id);
    }
    const nextWeights = { ...weights };
    if (!next.has(id)) delete nextWeights[id];
    onChange({
      ...params,
      formats: serializeFormats(next),
      formatWeights: serializeWeights(nextWeights, next),
    });
  };

  const setWeight = (id: string, w: number) => {
    const clamped = Math.max(1, Math.round(w));
    const nextWeights = { ...weights, [id]: clamped };
    onChange({
      ...params,
      formatWeights: serializeWeights(nextWeights, enabled),
    });
  };

  // Compute total weight for percentage display
  let totalWeight = 0;
  for (const f of ALL_FORMATS) {
    if (enabled.has(f.id)) {
      totalWeight += weights[f.id] ?? 1;
    }
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Format selection with weights */}
      <fieldset className="flex flex-col gap-1">
        <legend
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Log Formats
        </legend>
        <p
          className={`text-[0.7em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
        >
          Select which formats to generate and their relative weights. Higher
          weight means more frequent.
        </p>
        <div className="flex flex-col gap-1">
          {ALL_FORMATS.map((f) => {
            const isOn = enabled.has(f.id);
            const weight = weights[f.id] ?? 1;
            const pct =
              isOn && totalWeight > 0
                ? Math.round((weight / totalWeight) * 100)
                : 0;
            return (
              <div
                key={f.id}
                className={`flex items-center gap-3 px-3 py-2 rounded transition-colors ${c(
                  isOn ? "bg-ink-surface" : "bg-ink-well/50",
                  isOn ? "bg-light-surface" : "bg-light-well/50",
                )}`}
              >
                <Checkbox
                  checked={isOn}
                  onChange={() => toggleFormat(f.id)}
                  dark={dark}
                />

                {/* Label + description */}
                <div className="flex-1 min-w-0">
                  <span
                    className={`text-[0.85em] font-medium ${c(
                      isOn ? "text-text-bright" : "text-text-muted",
                      isOn ? "text-light-text-bright" : "text-light-text-muted",
                    )}`}
                  >
                    {f.label}
                  </span>
                  <span
                    className={`text-[0.75em] ml-2 ${c("text-text-muted", "text-light-text-muted")}`}
                  >
                    {f.description}
                  </span>
                </div>

                {/* Weight input + percentage */}
                {isOn && (
                  <div className="flex items-center gap-1.5 shrink-0">
                    <input
                      type="text"
                      inputMode="numeric"
                      value={weight}
                      onChange={(e) => {
                        const v = e.target.value;
                        if (v === "" || /^\d+$/.test(v))
                          setWeight(f.id, parseInt(v, 10) || 1);
                      }}
                      className={`w-10 px-1 py-0.5 text-[0.8em] font-mono text-center border rounded focus:outline-none transition-colors [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none ${c(
                        "bg-ink-well border-ink-border text-text-bright focus:border-copper-dim",
                        "bg-light-well border-light-border text-light-text-bright focus:border-copper",
                      )}`}
                    />
                    <span
                      className={`text-[0.7em] w-8 text-right font-mono ${c("text-text-muted", "text-light-text-muted")}`}
                    >
                      {pct}%
                    </span>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </fieldset>

      {/* Timing */}
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Min Interval"
          description="Minimum delay between messages"
          dark={dark}
        >
          <TextInput
            value={get("minInterval")}
            onChange={(v) => set("minInterval", v)}
            placeholder={d["minInterval"] ?? ""}
            dark={dark}
            mono
            examples={["50ms", "100ms", "500ms"]}
          />
        </FormField>
        <FormField
          label="Max Interval"
          description="Maximum delay between messages"
          dark={dark}
        >
          <TextInput
            value={get("maxInterval")}
            onChange={(v) => set("maxInterval", v)}
            placeholder={d["maxInterval"] ?? ""}
            dark={dark}
            mono
            examples={["500ms", "1s", "5s"]}
          />
        </FormField>
      </div>

      {/* Cardinality */}
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Host Count"
          description="Distinct hosts to simulate"
          dark={dark}
        >
          <NumberInput
            value={get("hostCount")}
            onChange={(v) => set("hostCount", v)}
            placeholder={d["hostCount"] ?? ""}
            dark={dark}
            min={1}
            examples={["5", "10", "50"]}
          />
        </FormField>
        <FormField
          label="Service Count"
          description="Distinct services to simulate"
          dark={dark}
        >
          <NumberInput
            value={get("serviceCount")}
            onChange={(v) => set("serviceCount", v)}
            placeholder={d["serviceCount"] ?? ""}
            dark={dark}
            min={1}
            examples={["3", "5", "20"]}
          />
        </FormField>
      </div>
    </div>
  );
}
