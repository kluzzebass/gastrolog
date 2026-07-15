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

interface RotationPolicyStepProps {
  dark: boolean;
  rotation: RotationData;
  onRotationChange: (data: RotationData) => void;
  rotationNamePlaceholder?: string;
}

interface RetentionPolicyStepProps {
  dark: boolean;
  retention: RetentionData;
  onRetentionChange: (data: RetentionData) => void;
  retentionNamePlaceholder?: string;
}

export function RotationPolicyStep({
  dark,
  rotation,
  onRotationChange,
  rotationNamePlaceholder,
}: Readonly<RotationPolicyStepProps>) {
  const c = useThemeClass(dark);
  return (
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
          placeholder={rotationNamePlaceholder || "default"}
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
  );
}

export function RetentionPolicyStep({
  dark,
  retention,
  onRetentionChange,
  retentionNamePlaceholder,
}: Readonly<RetentionPolicyStepProps>) {
  const c = useThemeClass(dark);
  return (
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
          placeholder={retentionNamePlaceholder || "default"}
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
  );
}
