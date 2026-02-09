import { useState } from "react";
import { useConfig, usePutIngester, useDeleteIngester } from "../../api/hooks";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { IngesterParamsForm } from "./IngesterParamsForm";

const ingesterTypes = [
  { value: "chatterbox", label: "chatterbox" },
  { value: "http", label: "http" },
  { value: "syslog", label: "syslog" },
];

export function IngestersSettings({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: config, isLoading } = useConfig();
  const putIngester = usePutIngester();
  const deleteIngester = useDeleteIngester();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  const [edits, setEdits] = useState<
    Record<string, { params: Record<string, string> }>
  >({});

  const [newId, setNewId] = useState("");
  const [newType, setNewType] = useState("chatterbox");
  const [newParams, setNewParams] = useState<Record<string, string>>({});

  const ingesters = config?.ingesters ?? [];

  const getEdit = (ing: { id: string; params: Record<string, string> }) => {
    const existing = edits[ing.id];
    if (existing) return existing;
    return { params: { ...ing.params } };
  };

  const setEdit = (id: string, params: Record<string, string>) => {
    setEdits((prev) => ({ ...prev, [id]: { params } }));
  };

  const handleSave = async (id: string, type: string) => {
    const edit = getEdit(ingesters.find((i) => i.id === id)!);
    try {
      await putIngester.mutateAsync({ id, type, params: edit.params });
      setEdits((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      addToast(`Ingester "${id}" updated`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update ingester", "error");
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteIngester.mutateAsync(id);
      addToast(`Ingester "${id}" deleted`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to delete ingester", "error");
    }
  };

  const handleNewTypeChange = (type: string) => {
    setNewType(type);
    setNewParams({});
  };

  const handleCreate = async () => {
    if (!newId.trim()) {
      addToast("Ingester ID is required", "warn");
      return;
    }
    try {
      await putIngester.mutateAsync({
        id: newId.trim(),
        type: newType,
        params: newParams,
      });
      addToast(`Ingester "${newId.trim()}" created`, "info");
      setAdding(false);
      setNewId("");
      setNewType("chatterbox");
      setNewParams({});
    } catch (err: any) {
      addToast(err.message ?? "Failed to create ingester", "error");
    }
  };

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
          Ingesters
        </h2>
        <button
          onClick={() => setAdding(!adding)}
          className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors"
        >
          {adding ? "Cancel" : "Add Ingester"}
        </button>
      </div>

      <div className="flex flex-col gap-3">
        {adding && (
          <div
            className={`border rounded-lg p-4 ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
          >
            <div className="flex flex-col gap-3">
              <div className="grid grid-cols-2 gap-3">
                <FormField label="ID" dark={dark}>
                  <TextInput
                    value={newId}
                    onChange={setNewId}
                    placeholder="my-ingester"
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField label="Type" dark={dark}>
                  <SelectInput
                    value={newType}
                    onChange={handleNewTypeChange}
                    options={ingesterTypes}
                    dark={dark}
                  />
                </FormField>
              </div>
              <IngesterParamsForm
                ingesterType={newType}
                params={newParams}
                onChange={setNewParams}
                dark={dark}
              />
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
                  disabled={putIngester.isPending}
                  className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                >
                  {putIngester.isPending ? "Creating..." : "Create"}
                </button>
              </div>
            </div>
          </div>
        )}

        {ingesters.map((ing) => {
          const edit = getEdit(ing);
          return (
            <SettingsCard
              key={ing.id}
              id={ing.id}
              typeBadge={ing.type}
              dark={dark}
              expanded={expanded === ing.id}
              onToggle={() => setExpanded(expanded === ing.id ? null : ing.id)}
              onDelete={() => handleDelete(ing.id)}
            >
              <div className="flex flex-col gap-3">
                <IngesterParamsForm
                  ingesterType={ing.type}
                  params={edit.params}
                  onChange={(p) => setEdit(ing.id, p)}
                  dark={dark}
                />
                <div className="flex justify-end pt-2">
                  <button
                    onClick={() => handleSave(ing.id, ing.type)}
                    disabled={putIngester.isPending}
                    className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                  >
                    {putIngester.isPending ? "Saving..." : "Save"}
                  </button>
                </div>
              </div>
            </SettingsCard>
          );
        })}

        {ingesters.length === 0 && !adding && (
          <div
            className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            No ingesters configured. Click "Add Ingester" to create one.
          </div>
        )}
      </div>
    </div>
  );
}
