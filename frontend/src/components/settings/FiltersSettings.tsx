import { useState, useCallback } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useConfig, usePutFilter, useDeleteFilter } from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput } from "./FormField";
import { PrimaryButton } from "./Buttons";
import { UsedByStatus, refsFor } from "./UsedByStatus";

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
  const _c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putFilter = usePutFilter();
  const deleteFilter = useDeleteFilter();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  const [newName, setNewName] = useState("");
  const [newExpression, setNewExpression] = useState("");

  const filters = config?.filters ?? [];
  const stores = config?.stores ?? [];

  const defaults = useCallback(
    (id: string) => {
      const fc = filters.find((f) => f.id === id);
      if (!fc) return { expression: "" };
      return { expression: fc.expression };
    },
    [filters],
  );

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveFilter, handleDelete } = useCrudHandlers({
    mutation: putFilter,
    deleteMutation: deleteFilter,
    label: "Filter",
    onSaveTransform: (id, edit: { expression: string }) => ({
      id,
      name: filters.find((f) => f.id === id)?.name ?? "",
      expression: edit.expression,
    }),
    onDeleteCheck: (id) => {
      const refs = refsFor(stores, "filter", id);
      return refs.length > 0
        ? `Filter "${id}" is referenced by store(s): ${refs.join(", ")}`
        : null;
    },
    clearEdit,
  });

  const handleSave = (id: string) => saveFilter(id, getEdit(id));

  const handleCreate = async () => {
    if (!newName.trim()) {
      addToast("Filter name is required", "warn");
      return;
    }
    try {
      await putFilter.mutateAsync({
        id: "",
        name: newName.trim(),
        expression: newExpression,
      });
      addToast(`Filter "${newName.trim()}" created`, "info");
      setAdding(false);
      setNewName("");
      setNewExpression("");
    } catch (err: any) {
      addToast(err.message ?? "Failed to create filter", "error");
    }
  };


  return (
    <SettingsSection
      title="Filters"
      helpTopicId="routing"
      addLabel="Add Filter"
      adding={adding}
      onToggleAdd={() => setAdding(!adding)}
      isLoading={isLoading}
      isEmpty={filters.length === 0}
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
          <FormField label="Name" dark={dark}>
            <TextInput
              value={newName}
              onChange={setNewName}
              placeholder="catch-all"
              dark={dark}
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

      {filters.map((fc) => {
        const id = fc.id;
        const edit = getEdit(id);
        const refs = refsFor(stores, "filter", id);
        return (
          <SettingsCard
            key={id}
            id={fc.name || id}
            dark={dark}
            expanded={expanded === id}
            onToggle={() => setExpanded(expanded === id ? null : id)}
            onDelete={() => handleDelete(id)}
            footer={
              <PrimaryButton
                onClick={() => handleSave(id)}
                disabled={putFilter.isPending || !isDirty(id)}
              >
                {putFilter.isPending ? "Saving..." : "Save"}
              </PrimaryButton>
            }
            status={<UsedByStatus dark={dark} refs={refs} />}
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
