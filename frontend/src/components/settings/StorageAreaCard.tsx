import type { StorageArea } from "../../api/gen/gastrolog/v1/storage_pb";
import { useEditState } from "../../hooks/useEditState";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, SpinnerInput } from "./FormField";
import { Button } from "./Buttons";

interface StorageAreaEdit {
  name: string;
  path: string;
  storageClass: string;
}

interface StorageAreaCardProps {
  area: StorageArea;
  nodeName: string;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onSave: (areaId: string, edit: StorageAreaEdit) => Promise<void>;
  onDelete: (areaId: string) => Promise<void>;
  saving: boolean;
}

export function StorageAreaCard({
  area,
  nodeName,
  dark,
  expanded,
  onToggle,
  onSave,
  onDelete,
  saving,
}: Readonly<StorageAreaCardProps>) {
  const defaults = (): StorageAreaEdit => ({
    name: area.name,
    path: area.path,
    storageClass: String(area.storageClass),
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(area.id);

  return (
    <SettingsCard
      id={area.name || area.id}
      typeBadge={`class ${area.storageClass}`}
      secondaryBadge={nodeName}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={() => onDelete(area.id)}
      deleteLabel="Remove"
      footer={
        <>
          {isDirty(area.id) && (
            <Button
              onClick={() => clearEdit(area.id)}
              disabled={saving}
              dark={dark}
              variant="ghost"
            >
              Discard
            </Button>
          )}
          <Button
            onClick={async () => {
              await onSave(area.id, edit);
              clearEdit(area.id);
            }}
            disabled={!isDirty(area.id) || saving}
            dark={dark}
          >
            {saving ? "Saving..." : "Save"}
          </Button>
        </>
      }
    >
      <div className="flex flex-col gap-3">
        <FormField label="Name" dark={dark}>
          <TextInput
            value={edit.name}
            onChange={(v) => setEdit(area.id, { name: v })}
            dark={dark}
          />
        </FormField>

        <FormField label="Path" dark={dark}>
          <TextInput
            value={edit.path}
            onChange={(v) => setEdit(area.id, { path: v })}
            dark={dark}
            mono
          />
        </FormField>

        <FormField label="Storage Class" dark={dark} description="Numeric rank. Lower = faster (e.g. 1 for NVMe, 3 for HDD).">
          <SpinnerInput
            value={edit.storageClass}
            onChange={(v) => setEdit(area.id, { storageClass: v })}
            dark={dark}
            min={0}
          />
        </FormField>
      </div>
    </SettingsCard>
  );
}
