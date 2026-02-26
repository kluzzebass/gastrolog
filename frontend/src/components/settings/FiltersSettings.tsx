import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useConfig, usePutFilter, useDeleteFilter, useGenerateName } from "../../api/hooks";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { useCrudHandlers } from "../../hooks/useCrudHandlers";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput } from "./FormField";
import { PrimaryButton } from "./Buttons";
import { UsedByStatus, refsFor } from "./UsedByStatus";
import type { SettingsTab } from "./SettingsDialog";

type NavigateTo = (tab: SettingsTab, entityName?: string) => void;

function FilterDescription({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const code = `font-mono text-[0.95em] px-1 py-px rounded ${c("bg-ink-well text-copper-dim", "bg-light-well text-copper")}`;
  return (
    <div className="flex flex-col gap-1.5">
      <p>
        Determines which ingested messages are vaulted. Each message's attributes
        are tested against this expression.
      </p>
      <div className="flex flex-col gap-1">
        <p>
          <span className={code}>*</span> catch-all — receives every message
        </p>
        <p>
          <span className={code}>+</span> catch-the-rest — receives messages
          that didn't match any other vault's filter
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
        Empty expression means the vault receives nothing. Token search (free
        text) is not supported in filters — only key=value attribute matching.
      </p>
    </div>
  );
}

export function FiltersSettings({ dark, onNavigateTo }: Readonly<{ dark: boolean; onNavigateTo?: NavigateTo }>) {
  const _c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putFilter = usePutFilter();
  const deleteFilter = useDeleteFilter();
  const { addToast } = useToast();

  const generateName = useGenerateName();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  const [newName, setNewName] = useState("");
  const [newExpression, setNewExpression] = useState("");

  const filters = config?.filters ?? [];
  const existingNames = new Set(filters.map((f) => f.name));
  const effectiveName = newName.trim() || "catch-all";
  const nameConflict = existingNames.has(effectiveName);
  const vaults = config?.vaults ?? [];

  const defaults = (id: string) => {
    const fc = filters.find((f) => f.id === id);
    if (!fc) return { name: "", expression: "" };
    return { name: fc.name, expression: fc.expression };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const { handleSave: saveFilter, handleDelete } = useCrudHandlers({
    mutation: putFilter,
    deleteMutation: deleteFilter,
    label: "Filter",
    onSaveTransform: (id, edit: { name: string; expression: string }) => ({
      id,
      name: edit.name,
      expression: edit.expression,
    }),
    onDeleteCheck: (id) => {
      const refs = refsFor(vaults, "filter", id);
      return refs.length > 0
        ? `Filter "${id}" is referenced by vault(s): ${refs.join(", ")}`
        : null;
    },
    clearEdit,
  });

  const handleSave = (id: string) => saveFilter(id, getEdit(id));

  const handleCreate = async () => {
    const name = newName.trim() || "catch-all";
    try {
      await putFilter.mutateAsync({
        id: "",
        name,
        expression: newExpression,
      });
      addToast(`Filter "${name}" created`, "info");
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
      onToggleAdd={() => {
        if (!adding) {
          generateName.mutateAsync().then((n) => setNewName(n));
        } else {
          setNewName("");
        }
        setAdding(!adding);
      }}
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
          createDisabled={nameConflict}
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
              examples={["*", "+", "env=prod"]}
            />
          </FormField>
        </AddFormCard>
      )}

      {filters.map((fc) => {
        const id = fc.id;
        const edit = getEdit(id);
        const refs = refsFor(vaults, "filter", id);
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
            status={<UsedByStatus dark={dark} refs={refs} onNavigate={onNavigateTo ? (name) => onNavigateTo("vaults", name) : undefined} />}
          >
            <div className="flex flex-col gap-3">
              <FormField label="Name" dark={dark}>
                <TextInput
                  value={edit.name}
                  onChange={(v) => setEdit(id, { name: v })}
                  dark={dark}
                />
              </FormField>
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
