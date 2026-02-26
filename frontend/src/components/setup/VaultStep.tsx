import { FormField, TextInput, SelectInput } from "../settings/FormField";
import { useThemeClass } from "../../hooks/useThemeClass";

export interface VaultData {
  name: string;
  type: string;
  dir: string;
}

interface VaultStepProps {
  dark: boolean;
  data: VaultData;
  onChange: (data: VaultData) => void;
}

const VAULT_TYPES = [
  { value: "file", label: "File (recommended)" },
  { value: "memory", label: "Memory (non-persistent)" },
];

export function VaultStep({ dark, data, onChange }: Readonly<VaultStepProps>) {
  const c = useThemeClass(dark);
  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-1">
        <h2
          className={`text-lg font-display font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Configure Vault
        </h2>
        <p
          className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
        >
          A vault holds your log data. Choose where and how logs are persisted. You can add more vaults later in Settings.
        </p>
      </div>

      <FormField label="Vault Name" dark={dark}>
        <TextInput
          value={data.name}
          onChange={(v) => onChange({ ...data, name: v })}
          placeholder="default"
          dark={dark}
        />
      </FormField>

      <FormField
        label="Vault Type"
        description="File vaults persist to disk. Memory vaults are fast but lost on restart."
        dark={dark}
      >
        <SelectInput
          value={data.type}
          onChange={(v) => onChange({ ...data, type: v })}
          options={VAULT_TYPES}
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
            dark={dark}
            mono
            examples={["/var/log/gastrolog/data", "/vaults"]}
          />
        </FormField>
      )}
    </div>
  );
}
