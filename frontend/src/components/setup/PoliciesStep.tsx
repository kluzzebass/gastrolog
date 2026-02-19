import { FormField, TextInput, NumberInput } from "../settings/FormField";
import { useThemeClass } from "../../hooks/useThemeClass";

export interface RotationData {
  name: string;
  maxAge: string;
  maxBytes: string;
  maxRecords: string;
  cron: string;
}

export interface RetentionData {
  name: string;
  maxChunks: string;
  maxAge: string;
  maxBytes: string;
}

interface PoliciesStepProps {
  dark: boolean;
  rotation: RotationData;
  retention: RetentionData;
  onRotationChange: (data: RotationData) => void;
  onRetentionChange: (data: RetentionData) => void;
}

/** Parse a human-friendly duration like "1h", "30m", "5m" to seconds. */
export function parseDurationToSeconds(s: string): bigint {
  const trimmed = s.trim();
  if (!trimmed) return BigInt(0);
  const match = trimmed.match(/^(\d+)\s*(s|m|h|d)$/i);
  if (!match) {
    // Try parsing as raw seconds.
    const n = parseInt(trimmed, 10);
    return isNaN(n) ? BigInt(0) : BigInt(n);
  }
  const num = parseInt(match[1]!, 10);
  switch (match[2]!.toLowerCase()) {
    case "s": return BigInt(num);
    case "m": return BigInt(num * 60);
    case "h": return BigInt(num * 3600);
    case "d": return BigInt(num * 86400);
    default: return BigInt(num);
  }
}

export function PoliciesStep({
  dark,
  rotation,
  retention,
  onRotationChange,
  onRetentionChange,
}: Readonly<PoliciesStepProps>) {
  const c = useThemeClass(dark);
  return (
    <div className="flex flex-col gap-6">
      {/* Rotation Policy */}
      <div className="flex flex-col gap-4">
        <div className="flex flex-col gap-1">
          <h2
            className={`text-lg font-display font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Rotation Policy
          </h2>
          <p
            className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Controls when active chunks are sealed and new ones created. Optional — skip if unsure.
          </p>
        </div>

        <FormField label="Policy Name" dark={dark}>
          <TextInput
            value={rotation.name}
            onChange={(v) => onRotationChange({ ...rotation, name: v })}
            placeholder="default"
            dark={dark}
          />
        </FormField>

        <div className="grid grid-cols-2 gap-3">
          <FormField
            label="Max Age"
            description="Seal chunk after this duration (e.g. 1h, 30m, 5m; optional)"
            dark={dark}
          >
            <TextInput
              value={rotation.maxAge}
              onChange={(v) => onRotationChange({ ...rotation, maxAge: v })}
              placeholder="1h"
              dark={dark}
              mono
            />
          </FormField>
          <FormField
            label="Max Bytes"
            description="Seal chunk after reaching this size (optional)"
            dark={dark}
          >
            <NumberInput
              value={rotation.maxBytes}
              onChange={(v) => onRotationChange({ ...rotation, maxBytes: v })}
              placeholder="0"
              dark={dark}
            />
          </FormField>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <FormField
            label="Max Records"
            description="Seal chunk after this many records (optional)"
            dark={dark}
          >
            <NumberInput
              value={rotation.maxRecords}
              onChange={(v) => onRotationChange({ ...rotation, maxRecords: v })}
              placeholder="0"
              dark={dark}
            />
          </FormField>
          <FormField
            label="Cron Schedule"
            description="Cron expression for periodic rotation (optional)"
            dark={dark}
          >
            <TextInput
              value={rotation.cron}
              onChange={(v) => onRotationChange({ ...rotation, cron: v })}
              placeholder=""
              dark={dark}
              mono
            />
          </FormField>
        </div>
      </div>

      {/* Divider */}
      <div
        className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
      />

      {/* Retention Policy */}
      <div className="flex flex-col gap-4">
        <div className="flex flex-col gap-1">
          <h2
            className={`text-lg font-display font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Retention Policy
          </h2>
          <p
            className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            Controls how long sealed chunks are kept before being deleted. Optional — skip if unsure.
          </p>
        </div>

        <FormField label="Policy Name" dark={dark}>
          <TextInput
            value={retention.name}
            onChange={(v) => onRetentionChange({ ...retention, name: v })}
            placeholder="default"
            dark={dark}
          />
        </FormField>

        <div className="grid grid-cols-2 gap-3">
          <FormField
            label="Max Chunks"
            description="Maximum number of sealed chunks to keep (optional)"
            dark={dark}
          >
            <NumberInput
              value={retention.maxChunks}
              onChange={(v) => onRetentionChange({ ...retention, maxChunks: v })}
              placeholder="10"
              dark={dark}
            />
          </FormField>
          <FormField
            label="Max Age"
            description="Delete chunks older than this (e.g. 7d, 24h; optional)"
            dark={dark}
          >
            <TextInput
              value={retention.maxAge}
              onChange={(v) => onRetentionChange({ ...retention, maxAge: v })}
              placeholder=""
              dark={dark}
              mono
            />
          </FormField>
        </div>

        <FormField
          label="Max Bytes"
          description="Maximum total size of retained chunks (optional)"
          dark={dark}
        >
          <NumberInput
            value={retention.maxBytes}
            onChange={(v) => onRetentionChange({ ...retention, maxBytes: v })}
            placeholder="0"
            dark={dark}
          />
        </FormField>
      </div>
    </div>
  );
}
