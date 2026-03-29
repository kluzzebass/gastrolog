import type { StorageArea } from "../../api/gen/gastrolog/v1/storage_pb";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useEditState } from "../../hooks/useEditState";
import { ExpandableCard } from "./ExpandableCard";
import { FormField, TextInput, SpinnerInput } from "./FormField";
import { Badge } from "../Badge";
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
  const c = useThemeClass(dark);

  const defaults = (): StorageAreaEdit => ({
    name: area.name,
    path: area.path,
    storageClass: String(area.storageClass),
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(area.id);

  return (
    <ExpandableCard
      id={area.name || area.id}
      typeBadge={`class ${area.storageClass}`}
      secondaryBadge={nodeName}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className={`text-[0.8em] font-mono ${c("text-text-muted", "text-light-text-muted")}`}>
          {area.path}
        </span>
      }
    >
      <div className="flex flex-col gap-3 p-4">
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

        <div className="flex items-center gap-2 pt-1">
          <Button
            onClick={async () => {
              await onSave(area.id, edit);
              clearEdit(area.id);
            }}
            disabled={!isDirty(area.id) || saving}
            dark={dark}
            variant="primary"
          >
            {saving ? "Saving..." : "Save"}
          </Button>
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
          <span className="flex-1" />
          <Button
            onClick={async () => {
              await onDelete(area.id);
            }}
            disabled={saving}
            dark={dark}
            variant="danger"
          >
            Remove
          </Button>
        </div>
      </div>
    </ExpandableCard>
  );
}
