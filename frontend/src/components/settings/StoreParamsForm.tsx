import { FormField, TextInput, SelectInput } from "./FormField";

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
        <FormField
          label="Timestamp precision"
          description="micro = 1µs resolution (default; fine for most logs). nano = 1ns resolution for high-resolution timing or ordering events within the same millisecond. Both use 8 bytes; nano has a smaller representable range (~±292 years from 1970) than micro (~±292k years)."
          dark={dark}
        >
          <SelectInput
            value={get("timestampPrecision") || "micro"}
            onChange={(v) => set("timestampPrecision", v)}
            options={[
              { value: "micro", label: "micro" },
              { value: "nano", label: "nano" },
            ]}
            dark={dark}
          />
        </FormField>
      </div>
    );
  }

  if (storeType === "memory") {
    return null;
  }

  return null;
}
