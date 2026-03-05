import { FormField, TextInput } from "./FormField";
import { Checkbox } from "./Checkbox";

interface VaultParamsFormProps {
  vaultType: string;
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
  vaultName?: string;
}

export function VaultParamsForm({
  vaultType,
  params,
  onChange,
  dark,
  vaultName,
}: Readonly<VaultParamsFormProps>) {
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });
  const get = (key: string) => params[key] ?? "";

  if (vaultType === "file") {
    return (
      <div className="flex flex-col gap-3">
        <FormField
          label="Directory"
          description="Path where chunk files are stored"
          dark={dark}
        >
          <TextInput
            value={get("dir")}
            onChange={(v) => set("dir", v)}
            placeholder={vaultName ? `vaults/${vaultName}` : ""}
            dark={dark}
            mono
          />
        </FormField>
        <Checkbox
          checked={get("compression") === "zstd"}
          onChange={(v) => set("compression", v ? "zstd" : "none")}
          label="Compress sealed chunks (zstd)"
          dark={dark}
        />
      </div>
    );
  }

  if (vaultType === "memory") {
    return null;
  }

  return null;
}
