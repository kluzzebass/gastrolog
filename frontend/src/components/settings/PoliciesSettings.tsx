import { useState } from "react";
import {
  useConfig,
  usePutRotationPolicy,
  useDeleteRotationPolicy,
} from "../../api/hooks";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, NumberInput } from "./FormField";
import { validateCron, describeCron } from "../../utils/cron";

// Format bytes for display.
function formatBytes(b: bigint): string {
  if (b === 0n) return "";
  if (b >= 1073741824n && b % 1073741824n === 0n) return `${b / 1073741824n}GB`;
  if (b >= 1048576n && b % 1048576n === 0n) return `${b / 1048576n}MB`;
  if (b >= 1024n && b % 1024n === 0n) return `${b / 1024n}KB`;
  return `${b}B`;
}

// Format seconds as human-readable duration.
function formatDuration(s: bigint): string {
  if (s === 0n) return "";
  const hours = s / 3600n;
  const mins = (s % 3600n) / 60n;
  const secs = s % 60n;
  if (hours > 0n && mins === 0n && secs === 0n) return `${hours}h`;
  if (hours > 0n && secs === 0n) return `${hours}h${mins}m`;
  if (mins > 0n && secs === 0n) return `${mins}m`;
  if (secs > 0n && hours === 0n && mins === 0n) return `${secs}s`;
  return `${hours > 0n ? `${hours}h` : ""}${mins > 0n ? `${mins}m` : ""}${secs > 0n ? `${secs}s` : ""}`;
}

// Parse a byte string like "64MB" to bigint.
function parseBytes(s: string): bigint {
  s = s.trim().toUpperCase();
  if (!s) return 0n;
  const match = s.match(/^(\d+)\s*(GB|MB|KB|B)?$/);
  if (!match) return 0n;
  const n = BigInt(match[1]!);
  switch (match[2]) {
    case "GB":
      return n * 1073741824n;
    case "MB":
      return n * 1048576n;
    case "KB":
      return n * 1024n;
    default:
      return n;
  }
}

// Parse a duration string like "5m" or "1h30m" to seconds as bigint.
function parseDuration(s: string): bigint {
  s = s.trim().toLowerCase();
  if (!s) return 0n;
  let total = 0n;
  const re = /(\d+)\s*(h|m|s)/g;
  let match;
  while ((match = re.exec(s)) !== null) {
    const n = BigInt(match[1]!);
    switch (match[2]) {
      case "h":
        total += n * 3600n;
        break;
      case "m":
        total += n * 60n;
        break;
      case "s":
        total += n;
        break;
    }
  }
  // If no unit matched, try as plain seconds.
  if (total === 0n && /^\d+$/.test(s)) total = BigInt(s);
  return total;
}

interface PolicyEdit {
  maxBytes: string;
  maxRecords: string;
  maxAge: string;
  cron: string;
}

function CronField({
  value,
  onChange,
  dark,
}: {
  value: string;
  onChange: (v: string) => void;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  const trimmed = value.trim();
  const validation = trimmed ? validateCron(trimmed) : null;
  const description = validation?.valid ? describeCron(trimmed) : null;

  return (
    <FormField
      label="Cron Schedule"
      description="cron: [sec] min hour dom mon dow"
      dark={dark}
    >
      <TextInput
        value={value}
        onChange={onChange}
        placeholder="0 * * * *"
        dark={dark}
        mono
      />
      {trimmed && validation && (
        <div className="mt-1 text-[0.75em]">
          {validation.valid ? (
            <span className={c("text-green-400", "text-green-600")}>
              {description}
            </span>
          ) : (
            <span className={c("text-red-400", "text-red-600")}>
              {validation.error}
            </span>
          )}
        </div>
      )}
    </FormField>
  );
}

export function PoliciesSettings({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putPolicy = usePutRotationPolicy();
  const deletePolicy = useDeleteRotationPolicy();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [edits, setEdits] = useState<Record<string, PolicyEdit>>({});

  const [newId, setNewId] = useState("");
  const [newMaxBytes, setNewMaxBytes] = useState("");
  const [newMaxRecords, setNewMaxRecords] = useState("");
  const [newMaxAge, setNewMaxAge] = useState("5m");
  const [newCron, setNewCron] = useState("");

  const policies = config?.rotationPolicies ?? {};
  const stores = config?.stores ?? [];

  const getEdit = (id: string): PolicyEdit => {
    if (edits[id]) return edits[id];
    const pol = policies[id];
    if (!pol) return { maxBytes: "", maxRecords: "", maxAge: "", cron: "" };
    return {
      maxBytes: formatBytes(pol.maxBytes),
      maxRecords: pol.maxRecords > 0n ? pol.maxRecords.toString() : "",
      maxAge: formatDuration(pol.maxAgeSeconds),
      cron: pol.cron,
    };
  };

  const setEdit = (id: string, patch: Partial<PolicyEdit>) => {
    setEdits((prev) => ({
      ...prev,
      [id]: { ...getEdit(id), ...prev[id], ...patch },
    }));
  };

  const handleSave = async (id: string) => {
    const edit = getEdit(id);
    if (edit.cron) {
      const result = validateCron(edit.cron);
      if (!result.valid) {
        addToast(`Invalid cron: ${result.error}`, "error");
        return;
      }
    }
    try {
      await putPolicy.mutateAsync({
        id,
        maxBytes: parseBytes(edit.maxBytes),
        maxRecords: edit.maxRecords ? BigInt(edit.maxRecords) : 0n,
        maxAgeSeconds: parseDuration(edit.maxAge),
        cron: edit.cron,
      });
      setEdits((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      addToast(`Policy "${id}" updated`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update policy", "error");
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deletePolicy.mutateAsync(id);
      const referencedBy = stores
        .filter((s) => s.policy === id)
        .map((s) => s.id);
      if (referencedBy.length > 0) {
        addToast(
          `Policy "${id}" deleted (was used by: ${referencedBy.join(", ")})`,
          "warn",
        );
      } else {
        addToast(`Policy "${id}" deleted`, "info");
      }
    } catch (err: any) {
      addToast(err.message ?? "Failed to delete policy", "error");
    }
  };

  const handleCreate = async () => {
    if (!newId.trim()) {
      addToast("Policy ID is required", "warn");
      return;
    }
    if (newCron) {
      const result = validateCron(newCron);
      if (!result.valid) {
        addToast(`Invalid cron: ${result.error}`, "error");
        return;
      }
    }
    try {
      await putPolicy.mutateAsync({
        id: newId.trim(),
        maxBytes: parseBytes(newMaxBytes),
        maxRecords: newMaxRecords ? BigInt(newMaxRecords) : 0n,
        maxAgeSeconds: parseDuration(newMaxAge),
        cron: newCron,
      });
      addToast(`Policy "${newId.trim()}" created`, "info");
      setAdding(false);
      setNewId("");
      setNewMaxBytes("");
      setNewMaxRecords("");
      setNewMaxAge("5m");
      setNewCron("");
    } catch (err: any) {
      addToast(err.message ?? "Failed to create policy", "error");
    }
  };

  // Which stores reference a policy.
  const refsFor = (policyId: string) =>
    stores.filter((s) => s.policy === policyId).map((s) => s.id);

  if (isLoading) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading...
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Rotation Policies
        </h2>
        <button
          onClick={() => setAdding(!adding)}
          className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors"
        >
          {adding ? "Cancel" : "Add Policy"}
        </button>
      </div>

      <div className="flex flex-col gap-3">
        {adding && (
          <div
            className={`border rounded-lg p-4 ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
          >
            <div className="flex flex-col gap-3">
              <FormField label="ID" dark={dark}>
                <TextInput
                  value={newId}
                  onChange={setNewId}
                  placeholder="default"
                  dark={dark}
                  mono
                />
              </FormField>
              <div className="grid grid-cols-3 gap-3">
                <FormField
                  label="Max Bytes"
                  description="e.g. 64MB, 1GB"
                  dark={dark}
                >
                  <TextInput
                    value={newMaxBytes}
                    onChange={setNewMaxBytes}
                    placeholder="64MB"
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField label="Max Records" dark={dark}>
                  <NumberInput
                    value={newMaxRecords}
                    onChange={setNewMaxRecords}
                    placeholder="100000"
                    dark={dark}
                  />
                </FormField>
                <FormField
                  label="Max Age"
                  description="e.g. 5m, 1h"
                  dark={dark}
                >
                  <TextInput
                    value={newMaxAge}
                    onChange={setNewMaxAge}
                    placeholder="5m"
                    dark={dark}
                    mono
                  />
                </FormField>
              </div>
              <CronField value={newCron} onChange={setNewCron} dark={dark} />
              <div className="flex justify-end gap-2 pt-2">
                <button
                  onClick={() => setAdding(false)}
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                    "border-ink-border text-text-muted hover:bg-ink-hover",
                    "border-light-border text-light-text-muted hover:bg-light-hover",
                  )}`}
                >
                  Cancel
                </button>
                <button
                  onClick={handleCreate}
                  disabled={putPolicy.isPending}
                  className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                >
                  {putPolicy.isPending ? "Creating..." : "Create"}
                </button>
              </div>
            </div>
          </div>
        )}

        {Object.entries(policies).map(([id, pol]) => {
          const edit = getEdit(id);
          const refs = refsFor(id);
          return (
            <SettingsCard
              key={id}
              id={id}
              dark={dark}
              expanded={expanded === id}
              onToggle={() => setExpanded(expanded === id ? null : id)}
              onDelete={() => handleDelete(id)}
              status={
                refs.length > 0 ? (
                  <span
                    className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                  >
                    used by: {refs.join(", ")}
                  </span>
                ) : undefined
              }
            >
              <div className="flex flex-col gap-3">
                <div className="grid grid-cols-3 gap-3">
                  <FormField
                    label="Max Bytes"
                    description="e.g. 64MB, 1GB"
                    dark={dark}
                  >
                    <TextInput
                      value={edit.maxBytes}
                      onChange={(v) => setEdit(id, { maxBytes: v })}
                      placeholder="64MB"
                      dark={dark}
                      mono
                    />
                  </FormField>
                  <FormField label="Max Records" dark={dark}>
                    <NumberInput
                      value={edit.maxRecords}
                      onChange={(v) => setEdit(id, { maxRecords: v })}
                      placeholder="100000"
                      dark={dark}
                    />
                  </FormField>
                  <FormField
                    label="Max Age"
                    description="e.g. 5m, 1h"
                    dark={dark}
                  >
                    <TextInput
                      value={edit.maxAge}
                      onChange={(v) => setEdit(id, { maxAge: v })}
                      placeholder="5m"
                      dark={dark}
                      mono
                    />
                  </FormField>
                </div>
                <CronField
                  value={edit.cron}
                  onChange={(v) => setEdit(id, { cron: v })}
                  dark={dark}
                />
                <div className="flex justify-end pt-2">
                  <button
                    onClick={() => handleSave(id)}
                    disabled={putPolicy.isPending}
                    className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                  >
                    {putPolicy.isPending ? "Saving..." : "Save"}
                  </button>
                </div>
              </div>
            </SettingsCard>
          );
        })}

        {Object.keys(policies).length === 0 && !adding && (
          <div
            className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            No rotation policies configured. Click "Add Policy" to create one.
          </div>
        )}
      </div>
    </div>
  );
}
