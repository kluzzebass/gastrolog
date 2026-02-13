import { useState, useCallback } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useConfig, usePutFilter, useDeleteFilter } from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput } from "./FormField";

function FilterDescription({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const code = `font-mono text-[0.95em] px-1 py-px rounded ${c("bg-ink-well text-copper-dim", "bg-light-well text-copper")}`;
  return (
    <div className="flex flex-col gap-1.5">
      <p>
        Determines which ingested messages are stored. Each message's attributes
        are tested against this expression.
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
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putFilter = usePutFilter();
  const deleteFilter = useDeleteFilter();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  const [newId, setNewId] = useState("");
  const [newExpression, setNewExpression] = useState("");

  const filters = config?.filters ?? {};
  const stores = config?.stores ?? [];

  const defaults = useCallback(
    (id: string) => {
      const fc = filters[id];
      if (!fc) return { expression: "" };
      return { expression: fc.expression };
    },
    [filters],
  );

  const { getEdit, setEdit, clearEdit } = useEditState(defaults);

  const handleSave = async (id: string) => {
    const edit = getEdit(id);
    try {
      await putFilter.mutateAsync({ id, expression: edit.expression });
      clearEdit(id);
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

  return (
    <SettingsSection
      title="Filters"
      addLabel="Add Filter"
      adding={adding}
      onToggleAdd={() => setAdding(!adding)}
      isLoading={isLoading}
      isEmpty={Object.keys(filters).length === 0}
      emptyMessage='No filters configured. Click "Add Filter" to create one.'
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => setAdding(false)}
          onCreate={handleCreate}
          isPending={putFilter.isPending}
        >
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
        </AddFormCard>
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
            footer={
              <button
                onClick={() => handleSave(id)}
                disabled={putFilter.isPending}
                className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
              >
                {putFilter.isPending ? "Saving..." : "Save"}
              </button>
            }
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
            </div>
          </SettingsCard>
        );
      })}
    </SettingsSection>
  );
}
