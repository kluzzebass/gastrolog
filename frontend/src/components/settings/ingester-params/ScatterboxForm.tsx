import { useState } from "react";
import { FormField, TextInput, NumberInput } from "../FormField";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { configClient } from "../../../api/client";
import type { SubFormProps } from "./types";

interface ScatterboxFormProps extends SubFormProps {
  ingesterId?: string;
}

export function ScatterboxForm({
  params,
  onChange,
  dark,
  defaults: d,
  ingesterId,
  ingesterNodeId,
}: Readonly<ScatterboxFormProps>) {
  const c = useThemeClass(dark);
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });
  const get = (key: string) => params[key] ?? "";

  const isOneShot = get("interval") === "0s" || get("interval") === "0ms" || get("interval") === "0";
  const [triggerState, setTriggerState] = useState<"idle" | "sent" | "error">("idle");

  const handleTrigger = async () => {
    if (!ingesterId) return;
    try {
      await configClient.triggerIngester(
        { id: ingesterId },
        ingesterNodeId ? { headers: { "X-Target-Node": ingesterNodeId } } : {},
      );
      setTriggerState("sent");
    } catch {
      setTriggerState("error");
    }
    setTimeout(() => setTriggerState("idle"), 1200);
  };

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-2 gap-3">
        <FormField
          label="Interval"
          description="Delay between emissions (0 = one-shot mode)"
          dark={dark}
        >
          <TextInput
            value={get("interval")}
            onChange={(v) => set("interval", v)}
            placeholder={d["interval"] ?? ""}
            dark={dark}
            mono
            examples={["0s", "1ms", "10ms", "100ms", "1s"]}
          />
        </FormField>
        <FormField
          label="Burst"
          description="Records per emission"
          dark={dark}
        >
          <NumberInput
            value={get("burst")}
            onChange={(v) => set("burst", v)}
            placeholder={d["burst"] ?? ""}
            dark={dark}
            min={1}
            examples={["1", "10", "100"]}
          />
        </FormField>
      </div>

      {ingesterId && (
        <button
          type="button"
          onClick={handleTrigger}
          disabled={triggerState !== "idle"}
          className={`self-start px-3 py-1.5 text-[0.8em] font-medium rounded transition-colors ${
            triggerState === "sent"
              ? "bg-green-600/20 text-green-400"
              : triggerState === "error"
                ? "bg-severity-error/20 text-severity-error"
                : c(
                    "bg-copper/15 text-copper hover:bg-copper/25",
                    "bg-copper/10 text-copper hover:bg-copper/20",
                  )
          }`}
        >
          {triggerState === "sent"
            ? "Burst Sent"
            : triggerState === "error"
              ? "Failed"
              : isOneShot ? "Emit Burst" : "Trigger Extra Burst"}
        </button>
      )}
    </div>
  );
}
