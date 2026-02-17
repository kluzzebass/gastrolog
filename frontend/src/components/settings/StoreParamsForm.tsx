import { FormField, TextInput } from "./FormField";
import { Checkbox } from "./Checkbox";

interface StoreParamsFormProps {
  storeType: string;
  params: Record<string, string>;
  onChange: (params: Record<string, string>) => void;
  dark: boolean;
}

export function StoreParamsForm({
  storeType,
  params,
  onChange,
  dark,
}: StoreParamsFormProps) {
  const set = (key: string, value: string) =>
    onChange({ ...params, [key]: value });
  const get = (key: string) => params[key] ?? "";

  if (storeType === "file") {
    return (
      <div className="flex flex-col gap-3">
        <FormField
          label="Directory"
          description="Path where chunk files are stored (required)"
          dark={dark}
        >
          <TextInput
            value={get("dir")}
            onChange={(v) => set("dir", v)}
            placeholder="/var/lib/gastrolog/data"
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

  if (storeType === "memory") {
    return null;
  }

  return null;
}
