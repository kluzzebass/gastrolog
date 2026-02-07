import { FormField, TextInput, NumberInput } from "./FormField";

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
    );
  }

  if (storeType === "memory") {
    return (
      <FormField
        label="Max Chunks"
        description="Maximum number of chunks kept in memory. Oldest are evicted when exceeded. Default 10."
        dark={dark}
      >
        <NumberInput
          value={get("maxChunks")}
          onChange={(v) => set("maxChunks", v)}
          placeholder="10"
          dark={dark}
          min={1}
        />
      </FormField>
    );
  }

  return null;
}
