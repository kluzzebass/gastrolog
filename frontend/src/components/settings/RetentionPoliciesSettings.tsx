import { useState } from "react";
import {
  useConfig,
  usePutRetentionPolicy,
  useDeleteRetentionPolicy,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, NumberInput } from "./FormField";

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
  const days = s / 86400n;
  const hours = (s % 86400n) / 3600n;
  const mins = (s % 3600n) / 60n;
  if (days > 0n && hours === 0n && mins === 0n) return `${days * 24n}h`;
  if (days > 0n && mins === 0n) return `${days * 24n + hours}h`;
  const totalHours = days * 24n + hours;
  if (totalHours > 0n && mins === 0n) return `${totalHours}h`;
  if (totalHours > 0n) return `${totalHours}h${mins}m`;
  if (mins > 0n) return `${mins}m`;
  return `${s}s`;
}

// Parse a byte string like "10GB" to bigint.
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

// Parse a duration string like "720h" or "30d" to seconds as bigint.
function parseDuration(s: string): bigint {
  s = s.trim().toLowerCase();
  if (!s) return 0n;
  let total = 0n;
  const re = /(\d+)\s*(d|h|m|s)/g;
  let match;
  while ((match = re.exec(s)) !== null) {
    const n = BigInt(match[1]!);
    switch (match[2]) {
      case "d":
        total += n * 86400n;
        break;
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
  if (total === 0n && /^\d+$/.test(s)) total = BigInt(s);
  return total;
}

interface PolicyEdit {
  maxAge: string;
  maxBytes: string;
  maxChunks: string;
}

export function RetentionPoliciesSettings({ dark }: { dark: boolean }) {
  const c = (d: string, l: string) => (dark ? d : l);
  const { data: config, isLoading } = useConfig();
  const putPolicy = usePutRetentionPolicy();
  const deletePolicy = useDeleteRetentionPolicy();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [edits, setEdits] = useState<Record<string, PolicyEdit>>({});

  const [newId, setNewId] = useState("");
  const [newMaxAge, setNewMaxAge] = useState("720h");
  const [newMaxBytes, setNewMaxBytes] = useState("");
  const [newMaxChunks, setNewMaxChunks] = useState("");

  const policies = config?.retentionPolicies ?? {};
  const stores = config?.stores ?? [];

  const getEdit = (id: string): PolicyEdit => {
    if (edits[id]) return edits[id];
    const pol = policies[id];
    if (!pol) return { maxAge: "", maxBytes: "", maxChunks: "" };
    return {
      maxAge: formatDuration(pol.maxAgeSeconds),
      maxBytes: formatBytes(pol.maxBytes),
      maxChunks: pol.maxChunks > 0n ? pol.maxChunks.toString() : "",
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
    try {
      await putPolicy.mutateAsync({
        id,
        maxAgeSeconds: parseDuration(edit.maxAge),
        maxBytes: parseBytes(edit.maxBytes),
        maxChunks: edit.maxChunks ? BigInt(edit.maxChunks) : 0n,
      });
      setEdits((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      addToast(`Retention policy "${id}" updated`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update retention policy", "error");
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deletePolicy.mutateAsync(id);
      const referencedBy = stores
        .filter((s) => s.retention === id)
        .map((s) => s.id);
      if (referencedBy.length > 0) {
        addToast(
          `Retention policy "${id}" deleted (was used by: ${referencedBy.join(", ")})`,
          "warn",
        );
      } else {
        addToast(`Retention policy "${id}" deleted`, "info");
      }
    } catch (err: any) {
      addToast(err.message ?? "Failed to delete retention policy", "error");
    }
  };

  const handleCreate = async () => {
    if (!newId.trim()) {
      addToast("Policy ID is required", "warn");
      return;
    }
    try {
      await putPolicy.mutateAsync({
        id: newId.trim(),
        maxAgeSeconds: parseDuration(newMaxAge),
        maxBytes: parseBytes(newMaxBytes),
        maxChunks: newMaxChunks ? BigInt(newMaxChunks) : 0n,
      });
      addToast(`Retention policy "${newId.trim()}" created`, "info");
      setAdding(false);
      setNewId("");
      setNewMaxAge("720h");
      setNewMaxBytes("");
      setNewMaxChunks("");
    } catch (err: any) {
      addToast(err.message ?? "Failed to create retention policy", "error");
    }
  };

  const refsFor = (policyId: string) =>
    stores.filter((s) => s.retention === policyId).map((s) => s.id);

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
          Retention Policies
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
                  label="Max Age"
                  description="e.g. 720h, 30d"
                  dark={dark}
                >
                  <TextInput
                    value={newMaxAge}
                    onChange={setNewMaxAge}
                    placeholder="720h"
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField
                  label="Max Bytes"
                  description="e.g. 10GB, 500MB"
                  dark={dark}
                >
                  <TextInput
                    value={newMaxBytes}
                    onChange={setNewMaxBytes}
                    placeholder="10GB"
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField label="Max Chunks" dark={dark}>
                  <NumberInput
                    value={newMaxChunks}
                    onChange={setNewMaxChunks}
                    placeholder="100"
                    dark={dark}
                  />
                </FormField>
              </div>
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
                    label="Max Age"
                    description="e.g. 720h, 30d"
                    dark={dark}
                  >
                    <TextInput
                      value={edit.maxAge}
                      onChange={(v) => setEdit(id, { maxAge: v })}
                      placeholder="720h"
                      dark={dark}
                      mono
                    />
                  </FormField>
                  <FormField
                    label="Max Bytes"
                    description="e.g. 10GB, 500MB"
                    dark={dark}
                  >
                    <TextInput
                      value={edit.maxBytes}
                      onChange={(v) => setEdit(id, { maxBytes: v })}
                      placeholder="10GB"
                      dark={dark}
                      mono
                    />
                  </FormField>
                  <FormField label="Max Chunks" dark={dark}>
                    <NumberInput
                      value={edit.maxChunks}
                      onChange={(v) => setEdit(id, { maxChunks: v })}
                      placeholder="100"
                      dark={dark}
                    />
                  </FormField>
                </div>
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
            No retention policies configured. Click "Add Policy" to create one.
          </div>
        )}
      </div>
    </div>
  );
}
