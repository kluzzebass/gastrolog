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

/** Parse a human-friendly byte size like "3GB", "512MB", "100KB" to bytes. */
export function parseBytesToBigInt(s: string): bigint {
  const trimmed = s.trim();
  if (!trimmed) return BigInt(0);
  const match = trimmed.match(/^(\d+)\s*(b|kb|mb|gb|tb)?$/i);
  if (!match) return BigInt(0);
  const num = BigInt(match[1]!);
  switch ((match[2] ?? "b").toLowerCase()) {
    case "b": return num;
    case "kb": return num * BigInt(1024);
    case "mb": return num * BigInt(1024 * 1024);
    case "gb": return num * BigInt(1024 * 1024 * 1024);
    case "tb": return num * BigInt(1024) * BigInt(1024 * 1024 * 1024);
    default: return num;
  }
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
            description="Seal chunk after this duration (optional)"
            dark={dark}
          >
            <TextInput
              value={rotation.maxAge}
              onChange={(v) => onRotationChange({ ...rotation, maxAge: v })}
              dark={dark}
              mono
              examples={["5m", "30m", "1h"]}
            />
          </FormField>
          <FormField
            label="Max Bytes"
            description="Seal chunk after reaching this size (optional)"
            dark={dark}
          >
            <TextInput
              value={rotation.maxBytes}
              onChange={(v) => onRotationChange({ ...rotation, maxBytes: v })}
              dark={dark}
              mono
              examples={["256MB", "512MB", "1GB"]}
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
              dark={dark}
              examples={["10000", "100000", "1000000"]}
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
              dark={dark}
              mono
              examples={["0 * * * *", "0 0 * * *"]}
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
              dark={dark}
              examples={["10", "50", "100"]}
            />
          </FormField>
          <FormField
            label="Max Age"
            description="Delete chunks older than this (optional)"
            dark={dark}
          >
            <TextInput
              value={retention.maxAge}
              onChange={(v) => onRetentionChange({ ...retention, maxAge: v })}
              dark={dark}
              mono
              examples={["7d", "30d", "90d"]}
            />
          </FormField>
        </div>

        <FormField
          label="Max Bytes"
          description="Maximum total size of retained chunks (optional)"
          dark={dark}
        >
          <TextInput
            value={retention.maxBytes}
            onChange={(v) => onRetentionChange({ ...retention, maxBytes: v })}
            dark={dark}
            mono
            examples={["1GB", "5GB", "10GB"]}
          />
        </FormField>
      </div>
    </div>
  );
}
