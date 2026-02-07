import { useState } from "react";
import {
  useConfig,
  usePutFilter,
  useDeleteFilter,
} from "../../api/hooks";
import { useToast } from "../Toast";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput } from "./FormField";

function FilterDescription({ dark }: { dark: boolean }) {
  const c = (d: string, l: string) => (dark ? d : l);
  const code = `font-mono text-[0.95em] px-1 py-px rounded ${c("bg-ink-well text-copper-dim", "bg-light-well text-copper")}`;
  return (
    <div className="flex flex-col gap-1.5">
      <p>
        Determines which ingested messages are stored. Each message's
        attributes are tested against this expression.
      </p>
      <div className="flex flex-col gap-1">
        <p>
          <span className={code}>*</span> catch-all — receives every message
        </p>
        <p>
          <span className={code}>+</span> catch-the-rest — receives messages
          that didn't match any other store's filter
        </p>
        <p>
          <span className={code}>key=value</span> attribute match — e.g.{" "}
          <span className={code}>env=prod</span>
        </p>
        <p>
          Supports boolean expressions:{" "}
          <span className={code}>env=prod AND level=error</span>,{" "}
          <span className={code}>(level=error OR level=warn)</span>
        </p>
      </div>
      <p className={c("text-text-ghost", "text-light-text-ghost")}>
        Empty expression means the store receives nothing. Token search (free
        text) is not supported in filters — only key=value attribute matching.
      </p>
    </div>
  );
}

export function FiltersSettings({ dark }: { dark: boolean }) {
  const c = (d: string, l: string) => (dark ? d : l);
  const { data: config, isLoading } = useConfig();
  const putFilter = usePutFilter();
  const deleteFilter = useDeleteFilter();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [edits, setEdits] = useState<Record<string, { expression: string }>>({});

  const [newId, setNewId] = useState("");
  const [newExpression, setNewExpression] = useState("");

  const filters = config?.filters ?? {};
  const stores = config?.stores ?? [];

  const getEdit = (id: string): { expression: string } => {
    if (edits[id]) return edits[id];
    const fc = filters[id];
    if (!fc) return { expression: "" };
    return { expression: fc.expression };
  };

  const setEdit = (id: string, patch: Partial<{ expression: string }>) => {
    setEdits((prev) => ({
      ...prev,
      [id]: { ...getEdit(id), ...prev[id], ...patch },
    }));
  };

  const handleSave = async (id: string) => {
    const edit = getEdit(id);
    try {
      await putFilter.mutateAsync({ id, expression: edit.expression });
      setEdits((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      addToast(`Filter "${id}" updated`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update filter", "error");
    }
  };

  const handleDelete = async (id: string) => {
    const referencedBy = stores.filter((s) => s.filter === id).map((s) => s.id);
    if (referencedBy.length > 0) {
      addToast(
        `Filter "${id}" is referenced by store(s): ${referencedBy.join(", ")}`,
        "warn",
      );
      return;
    }
    try {
      await deleteFilter.mutateAsync(id);
      addToast(`Filter "${id}" deleted`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to delete filter", "error");
    }
  };

  const handleCreate = async () => {
    if (!newId.trim()) {
      addToast("Filter ID is required", "warn");
      return;
    }
    try {
      await putFilter.mutateAsync({
        id: newId.trim(),
        expression: newExpression,
      });
      addToast(`Filter "${newId.trim()}" created`, "info");
      setAdding(false);
      setNewId("");
      setNewExpression("");
    } catch (err: any) {
      addToast(err.message ?? "Failed to create filter", "error");
    }
  };

  const refsFor = (filterId: string) =>
    stores.filter((s) => s.filter === filterId).map((s) => s.id);

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
          Filters
        </h2>
        <button
          onClick={() => setAdding(!adding)}
          className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors"
        >
          {adding ? "Cancel" : "Add Filter"}
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
                  placeholder="catch-all"
                  dark={dark}
                  mono
                />
              </FormField>
              <FormField
                label="Expression"
                description={<FilterDescription dark={dark} />}
                dark={dark}
              >
                <TextInput
                  value={newExpression}
                  onChange={setNewExpression}
                  placeholder="*"
                  dark={dark}
                  mono
                />
              </FormField>
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
                  disabled={putFilter.isPending}
                  className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                >
                  {putFilter.isPending ? "Creating..." : "Create"}
                </button>
              </div>
            </div>
          </div>
        )}

        {Object.entries(filters).map(([id]) => {
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
                    className={`text-[0.7em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                  >
                    used by: {refs.join(", ")}
                  </span>
                ) : undefined
              }
            >
              <div className="flex flex-col gap-3">
                <FormField
                  label="Expression"
                  description={<FilterDescription dark={dark} />}
                  dark={dark}
                >
                  <TextInput
                    value={edit.expression}
                    onChange={(v) => setEdit(id, { expression: v })}
                    dark={dark}
                    mono
                  />
                </FormField>
                <div className="flex justify-end pt-2">
                  <button
                    onClick={() => handleSave(id)}
                    disabled={putFilter.isPending}
                    className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                  >
                    {putFilter.isPending ? "Saving..." : "Save"}
                  </button>
                </div>
              </div>
            </SettingsCard>
          );
        })}

        {Object.keys(filters).length === 0 && !adding && (
          <div
            className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            No filters configured. Click "Add Filter" to create one.
          </div>
        )}
      </div>
    </div>
  );
}
