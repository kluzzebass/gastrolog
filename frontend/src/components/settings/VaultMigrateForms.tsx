import { encode } from "../../api/glid";
import { useThemeClass } from "../../hooks/useThemeClass";
import { FormField, TextInput, SelectInput } from "./FormField";
import { Button } from "./Buttons";
import type { VaultConfig } from "../../api/gen/gastrolog/v1/system_pb";

interface MigrateTarget {
  name: string;
  type: string;
  dir: string;
}

export function MigrateVaultForm({
  dark,
  vault: _vault,
  target,
  isPending,
  activeJob,
  onTargetChange,
  onSubmit,
}: Readonly<{
  dark: boolean;
  vault: Pick<VaultConfig, "id" | "name">;
  target: MigrateTarget;
  isPending: boolean;
  activeJob: boolean;
  onTargetChange: (update: Partial<MigrateTarget>) => void;
  onSubmit: () => void;
}>) {
  const c = useThemeClass(dark);
  const dirRequired = target.type === "file";
  const canSubmit = target.name.trim() && (!dirRequired || target.dir.trim());

  return (
    <div
      className={`flex flex-col gap-3 p-3 rounded border ${c(
        "border-ink-border-subtle bg-ink-raised",
        "border-light-border-subtle bg-light-bg",
      )}`}
    >
      <div
        className={`text-[0.75em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Migrate Vault
      </div>
      <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
        Creates a new destination vault, disables this vault so no new data flows in, then moves all records to the destination and deletes this vault.
      </p>
      <div className="grid grid-cols-3 gap-3">
        <FormField label="Destination Name" dark={dark}>
          <TextInput
            value={target.name}
            onChange={(v) => onTargetChange({ name: v })}
            placeholder="new-vault"
            dark={dark}
            mono
          />
        </FormField>
        <FormField label="Type" dark={dark}>
          <SelectInput
            value={target.type}
            onChange={(v) => onTargetChange({ type: v, dir: "" })}
            options={[
              { value: "", label: "(same)" },
              { value: "memory", label: "memory" },
              { value: "file", label: "file" },
            ]}
            dark={dark}
          />
        </FormField>
        {dirRequired && (
          <FormField label="Directory" dark={dark}>
            <TextInput
              value={target.dir}
              onChange={(v) => onTargetChange({ dir: v })}
              placeholder="/path/to/vault"
              dark={dark}
              mono
              examples={["/var/lib/gastrolog/data"]}
            />
          </FormField>
        )}
      </div>
      <div className="flex justify-end">
        <Button
          disabled={isPending || !canSubmit || activeJob}
          onClick={onSubmit}
        >
          {isPending ? "Migrating..." : "Migrate"}
        </Button>
      </div>
    </div>
  );
}

export function MergeVaultForm({
  dark,
  vault,
  selectedDestination,
  vaults,
  isPending,
  activeJob,
  onDestinationChange,
  onSubmit,
}: Readonly<{
  dark: boolean;
  vault: Pick<VaultConfig, "id" | "name">;
  selectedDestination: string;
  vaults: Pick<VaultConfig, "id" | "name">[];
  isPending: boolean;
  activeJob: boolean;
  onDestinationChange: (id: string) => void;
  onSubmit: () => void;
}>) {
  const c = useThemeClass(dark);

  return (
    <div
      className={`flex flex-col gap-3 p-3 rounded border ${c(
        "border-ink-border-subtle bg-ink-raised",
        "border-light-border-subtle bg-light-bg",
      )}`}
    >
      <div
        className={`text-[0.75em] font-medium uppercase tracking-[0.15em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Merge Into Another Vault
      </div>
      <p className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
        Disables this vault, moves all records into the destination, then deletes this vault.
      </p>
      <div className="grid grid-cols-2 gap-3">
        <FormField label="Destination" dark={dark}>
          <SelectInput
            value={selectedDestination}
            onChange={onDestinationChange}
            options={[
              { value: "", label: "(select)" },
              ...vaults
                .filter((s) => encode(s.id) !== encode(vault.id))
                .map((s) => ({ value: encode(s.id), label: s.name || encode(s.id) }))
                .sort((a, b) => a.label.localeCompare(b.label)),
            ]}
            dark={dark}
          />
        </FormField>
      </div>
      <div className="flex justify-end">
        <Button
          disabled={isPending || !selectedDestination || activeJob}
          onClick={onSubmit}
        >
          {isPending ? "Merging..." : "Merge"}
        </Button>
      </div>
    </div>
  );
}
