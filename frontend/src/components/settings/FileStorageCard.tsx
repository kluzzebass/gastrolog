import { encode } from "../../api/glid";
import type { FileStorage } from "../../api/gen/gastrolog/v1/storage_pb";
import { useEditState } from "../../hooks/useEditState";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, SpinnerInput } from "./FormField";
import { Button } from "./Buttons";

interface FileStorageEdit {
  name: string;
  path: string;
  storageClass: string;
}

interface FileStorageCardProps {
  fs: FileStorage;
  nodeName: string;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onSave: (storageId: string, edit: FileStorageEdit) => Promise<void>;
  onDelete: (storageId: string) => Promise<void>;
  saving: boolean;
}

export function FileStorageCard({
  fs,
  nodeName,
  dark,
  expanded,
  onToggle,
  onSave,
  onDelete,
  saving,
}: Readonly<FileStorageCardProps>) {
  const defaults = (): FileStorageEdit => ({
    name: fs.name,
    path: fs.path,
    storageClass: String(fs.storageClass),
  });

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);
  const edit = getEdit(encode(fs.id));

  return (
    <SettingsCard
      id={fs.name || encode(fs.id)}
      typeBadge={`class ${fs.storageClass}`}
      secondaryBadge={nodeName}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      onDelete={() => onDelete(encode(fs.id))}
      deleteLabel="Remove"
      footer={
        <>
          {isDirty(encode(fs.id)) && (
            <Button
              onClick={() => clearEdit(encode(fs.id))}
              disabled={saving}
              dark={dark}
              variant="ghost"
            >
              Discard
            </Button>
          )}
          <Button
            onClick={async () => {
              await onSave(encode(fs.id), edit);
              clearEdit(encode(fs.id));
            }}
            disabled={!isDirty(encode(fs.id)) || saving}
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
            onChange={(v) => setEdit(encode(fs.id), { name: v })}
            dark={dark}
          />
        </FormField>

        <FormField label="Path" dark={dark}>
          <TextInput
            value={edit.path}
            onChange={(v) => setEdit(encode(fs.id), { path: v })}
            dark={dark}
            mono
          />
        </FormField>

        <FormField label="Storage Class" dark={dark} description="Numeric rank. Lower = faster (e.g. 1 for NVMe, 3 for HDD).">
          <SpinnerInput
            value={edit.storageClass}
            onChange={(v) => setEdit(encode(fs.id), { storageClass: v })}
            dark={dark}
            min={0}
          />
        </FormField>
      </div>
    </SettingsCard>
  );
}
