import { FormField, TextInput, SelectInput } from "../settings/FormField";
import { useThemeClass } from "../../hooks/useThemeClass";

export interface StoreData {
  name: string;
  type: string;
  dir: string;
}

interface StoreStepProps {
  dark: boolean;
  data: StoreData;
  onChange: (data: StoreData) => void;
}

const STORE_TYPES = [
  { value: "file", label: "File (recommended)" },
  { value: "memory", label: "Memory (non-persistent)" },
];

export function StoreStep({ dark, data, onChange }: Readonly<StoreStepProps>) {
  const c = useThemeClass(dark);
  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-1">
        <h2
          className={`text-lg font-display font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Configure Store
        </h2>
        <p
          className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          A store holds your log data. Choose where and how logs are persisted. You can add more stores later in Settings.
        </p>
      </div>

      <FormField label="Store Name" dark={dark}>
        <TextInput
          value={data.name}
          onChange={(v) => onChange({ ...data, name: v })}
          placeholder="default"
          dark={dark}
        />
      </FormField>

      <FormField
        label="Store Type"
        description="File stores persist to disk. Memory stores are fast but lost on restart."
        dark={dark}
      >
        <SelectInput
          value={data.type}
          onChange={(v) => onChange({ ...data, type: v })}
          options={STORE_TYPES}
          dark={dark}
        />
      </FormField>

      {data.type === "file" && (
        <FormField
          label="Directory"
          description="Directory where log chunks will be stored."
          dark={dark}
        >
          <TextInput
            value={data.dir}
            onChange={(v) => onChange({ ...data, dir: v })}
            placeholder="/var/lib/gastrolog/data"
            dark={dark}
            mono
          />
        </FormField>
      )}
    </div>
  );
}
